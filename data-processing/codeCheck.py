import os
import zipfile
import requests
import io
import pandas as pd
import sqlalchemy
from sqlalchemy import text


#   This file is to check if the codes from the JSON file with the mapping (codes --> description)
#   are all present in the codes from the CMS database (and vice versa).
#
#   If any codes are missing, we need to add them, and if there are any in the JSOn file
#   that aren't in the database, then we can get rid of it.


DB_URL = "postgresql://postgres:NewPassword!@localhost:5432/postgres"
LOCAL_ZIP_PATH = "../data/raw/hcpc2025/HCPC2025_OCT_ANWEB_Transaction Report_v4.xlsx"
HCPCS_TABLE = "hcpcs_reference"
MAIN_TABLE = "zip_stats_df"
MAIN_TABLE_CODE_COLUMN = "HCPCS_Cd"
MAIN_TABLE_DESC_COLUMN = "HCPCS_Descr"


# === 1. Load .xlxs file into a df ===
hcpcs_df = pd.read_excel(LOCAL_ZIP_PATH, usecols=["HCPC", "LONG DESCRIPTION", "SHORT DESCRIPTION"])
print("Loaded HCPCS file with shape", hcpcs_df.head)


# === 2. Write to PostgreSQL as reference table ===
engine = sqlalchemy.create_engine(DB_URL)
hcpcs_df.to_sql(HCPCS_TABLE, engine, if_exists="replace", index=False)
print("Wrote HCPCS reference table!")


# === 3. Identify missing codes in your main table ===
with engine.connect() as conn:
    # Get unique/distinct codes from the table.
    codes_main = pd.read_sql(f"SELECT DISTINCT \"{MAIN_TABLE_CODE_COLUMN}\" as code FROM {MAIN_TABLE}", conn)
    codes_ref  = pd.read_sql(f"SELECT DISTINCT \"HCPC\" as code FROM {HCPCS_TABLE}", conn)
    result = conn.execute(text(f"SELECT COUNT(DISTINCT \"{MAIN_TABLE_CODE_COLUMN}\") FROM {MAIN_TABLE}"))
    count = result.scalar()
    print(count)
codes_main_set = set(codes_main['code'].dropna())
codes_ref_set  = set(codes_ref['code'].dropna())
missing_codes = sorted(list(codes_main_set - codes_ref_set))
print(f"Found {len(missing_codes)} missing HCPCS codes")


# === 4. (Optional) Use LLM to generate descriptions for missing codes ===
# Pseudo-code — adapt with your LLM API (e.g., OpenAI, Gemini, etc.)
generated = []
for code in missing_codes:
    prompt = f"Describe the HCPCS Level II code {code}. Provide a short description."
    # api_result = call_your_llm_api(prompt)
    api_result = f"Generated description for {code}"  # placeholder
    generated.append((code, api_result))

gen_df = pd.DataFrame(generated, columns=['hcpcs_cd','short_description'])
print("Generated descriptions shape:", gen_df.shape)


"""
# Write to another table
GEN_TABLE = "hcpcs_generated"
gen_df.to_sql(GEN_TABLE, engine, if_exists="replace", index=False)

# === 6. Merge reference + generated descriptions, then update main table ===
# Create a full mapping table
with engine.connect() as conn:
    # join reference + generated
    conn.execute(text(f#
        CREATE TABLE hcpcs_full AS
        SELECT
            r.hcpcs_cd,
            COALESCE(r.long_description, r.short_description, g.short_description) AS descr
        FROM {HCPCS_TABLE} r
        LEFT JOIN {GEN_TABLE} g
            ON r.hcpcs_cd = g.hcpcs_cd
        UNION
        SELECT
            g.hcpcs_cd,
            g.short_description AS descr
        FROM {GEN_TABLE} g
        WHERE g.hcpcs_cd NOT IN (SELECT hcpcs_cd FROM {HCPCS_TABLE})
    #))
    # add new column to main table if not exists
    conn.execute(text(f#ALTER TABLE {MAIN_TABLE}
                          ADD COLUMN IF NOT EXISTS "{MAIN_TABLE_DESC_COLUMN}" TEXT;#))
    # update main table by joining
    conn.execute(text(f#
        UPDATE {MAIN_TABLE} m
        SET "{MAIN_TABLE_DESC_COLUMN}" = f.descr
        FROM hcpcs_full f
        WHERE m."{MAIN_TABLE_CODE_COLUMN}" = f.hcpcs_cd;
    #))
print("Main table updated with HCPCS descriptions.")
"""