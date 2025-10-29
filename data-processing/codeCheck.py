from dotenv import load_dotenv
from sqlalchemy import text
from google import genai
import pandas as pd
import sqlalchemy
import json
import os


load_dotenv()
API_KEY = os.getenv("API_KEY")
client = genai.Client(api_key=API_KEY)


#   This file is to check if the codes from the JSON file with the mapping (codes --> description)
#   are all present in the codes from the CMS database (and vice versa).

#   If any codes are missing, we need to add them, and if there are any in the JSOn file
#   that aren't in the database, then we can get rid of it.


DB_URL = "postgresql://postgres:NewPassword!@localhost:5432/postgres"
LOCAL_ZIP_PATH = "../data/raw/hcpc2025/HCPC2025_OCT_ANWEB_Transaction Report_v4.xlsx"
HCPCS_TABLE = "hcpcs_reference"
MAIN_TABLE = "zip_stats_df"
MAIN_TABLE_CODE_COLUMN = "HCPCS_Cd"
MAIN_TABLE_DESC_COLUMN = "HCPCS_Description"


# === 1. Load .xlxs file into a df ===
hcpcs_df = pd.read_excel(LOCAL_ZIP_PATH, usecols=["HCPC", "LONG DESCRIPTION", "SHORT DESCRIPTION"])
print("Loaded HCPCS file with shape", hcpcs_df.head())



# === 2. Write to PostgreSQL as reference table ===
engine = sqlalchemy.create_engine(DB_URL)
hcpcs_df.to_sql(HCPCS_TABLE, engine, if_exists="replace", index=False)
print("Wrote HCPCS reference table!")



# === 3. Identify missing codes in your main table ===
with engine.connect() as conn:
    # Get unique/distinct codes from the table.
    codes_main = pd.read_sql(f"SELECT DISTINCT \"{MAIN_TABLE_CODE_COLUMN}\" as code FROM {MAIN_TABLE}", conn)
    codes_ref  = pd.read_sql(f"SELECT DISTINCT \"HCPC\" as code FROM {HCPCS_TABLE}", conn)

codes_main_set = set(codes_main['code'].dropna())
codes_ref_set  = set(codes_ref['code'].dropna())
missing_codes = sorted(list(codes_main_set - codes_ref_set))
print(f"Found {len(missing_codes)} missing HCPCS codes")
print(f"...meaning there are {len(codes_main_set)-len(missing_codes)} codes that we DO have. Yeah...not a lot.")



# === 4. Use LLM to generate descriptions for missing codes ===
# We're going to batch process codes, feeding them into Gemini and returning
# solely the code + description in JSON format, parsing this later to feed
# into the DB.

batch_size = 100  # 5,000 total codes, so going to create batches right now.
results = {}
for i in range(0, len(missing_codes), batch_size):
    batch = missing_codes[i:i + batch_size]
    prompt = (f"""
            You are a medical coding assistant.
            Provide a short (max 15 words) description for each HCPCS (or similar) code below. 
            Respond strictly as JSON mapping codes to descriptions, like so:
            [ "A0428": "Ambulance service, basic life support", "J3490": "Unclassified drugs", ... ]
            
            Codes:
            {batch}
            """)

    #Get the response from the prompt.
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )

    try:
        json_str = response.text.strip()
        # Sometimes Gemini wraps JSON in ```json ... ``` blocks — clean that:
        json_str = json_str.removeprefix("```json").removesuffix("```").strip()

        # Ensure starts with { and ends with }
        if json_str.startswith("["):
            json_str = json_str.replace("[", "{", 1)
        if json_str.endswith("]"):
            json_str = json_str[::-1].replace("]", "}", 1)[::-1]

        data = json.loads(json_str)
        results.update(data)
    except Exception as e:
        print(f"Batch {i} JSON parse failed: {e}\nReturned text:\n{json_str[:200]}...")

# Convert results to DataFrame.
generatedDF = pd.DataFrame(list(results.items()), columns=["HCPCS_Cd", "short_description"])
generatedDF.to_json("../data/raw/codes_generated.json", orient='records', indent=4)

# Write to another table
GEN_TABLE = "hcpcs_generated"
generatedDF.to_sql(GEN_TABLE, engine, if_exists="replace", index=False)



# === 5. Merge reference + generated descriptions, then update main table ===
# Create a full mapping table
with engine.begin() as conn:
    # join reference + generated
    conn.execute(text(f"""
        CREATE TABLE hcpcs_full AS
        SELECT
            r."HCPC" AS hcpcs_cd,
            COALESCE(r."LONG DESCRIPTION", r."SHORT DESCRIPTION", g.short_description) AS descr
        FROM {HCPCS_TABLE} r
        LEFT JOIN {GEN_TABLE} g
            ON r."HCPC" = g."HCPCS_Cd"
        UNION
        SELECT
            g."HCPCS_Cd",
            g.short_description AS descr
        FROM {GEN_TABLE} g
        WHERE g."HCPCS_Cd" NOT IN (SELECT "HCPC" FROM {HCPCS_TABLE})
    """))
    # add new column to main table if not exists
    conn.execute(text(f"""ALTER TABLE {MAIN_TABLE}
                          ADD COLUMN IF NOT EXISTS "{MAIN_TABLE_DESC_COLUMN}" TEXT;"""))
    #update main table by joining
    conn.execute(text(f"""
        UPDATE {MAIN_TABLE} m
        SET "{MAIN_TABLE_DESC_COLUMN}" = f.descr
        FROM hcpcs_full f
        WHERE m."{MAIN_TABLE_CODE_COLUMN}" = f.hcpcs_cd;
    """))

print("Main table updated with HCPCS descriptions.")