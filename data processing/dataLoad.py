from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os


#Currently ~9 million rows, takes a long time and strong processing to do this, so only doing around like 200 right now.
#We're going to keep the Zip Code, visit code, the amount they paid, and the amount the amount medicare wanted to be paid.
baseDirectory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
cmsData = os.path.join(baseDirectory, 'data', 'raw', 'Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2022.csv')
hcpcsCodesData = os.path.join(baseDirectory, 'data', 'processed', 'HCPS Codes - Sheet1.csv')


data = pd.read_csv(cmsData, usecols=["Rndrng_NPI", "Rndrng_Prvdr_Zip5", "HCPCS_Cd", "HCPCS_Desc", "Tot_Benes", "Tot_Srvcs", "Avg_Sbmtd_Chrg", "Avg_Mdcr_Alowd_Amt", "Avg_Mdcr_Pymt_Amt"], nrows=10000)
hcpcsCodes = pd.read_csv(hcpcsCodesData)
data["Estimated_Average_Payment"] = data["Avg_Mdcr_Alowd_Amt"] - data["Avg_Mdcr_Pymt_Amt"]
print(data.shape)


#Add the codes to a hashmap, simply noting what each code means in an easily accessible format.
hcpcsHash = {}
codeIndex = 0
for code in hcpcsCodes["HCPCS Code"]:
    hcpcsHash.update({code: hcpcsCodes["Description"][codeIndex]})
    codeIndex += 1


#1. Remove ANY NaN rows from the entire dataframe (aka if anything is missing in a row, then it shouldn't be included).
data = data.dropna()


#2. Checking if any Zip codes are incorrect format (either too long, too short, 99999, or 00000)
#   Check if the code in the data is one of the defined HCPCS Codes listed. If not, remove.
#   Will continue to expand the number of codes available, otherwise this is a limiting factor.
rowIndex = 0
for _, row in data.iterrows():
    zip = str(row["Rndrng_Prvdr_Zip5"])
    code = row["HCPCS_Cd"]

    #Run through all these parts; if the row is NOT dropped, then we can use it in our values for the averages.
    if zip == ("99999" or "00000"):
        data.drop(rowIndex)
    elif len(zip) < 5 or len(zip) > 5:
        data.drop(rowIndex)
    elif code not in hcpcsHash:
        data.drop(rowIndex)

    rowIndex += 1


#3. Group all the charges by their zip codes and then return the average of each one, giving us the the average
#   submitted, medicare allowed, medicare payment, and patient payment per zip code. Then, concatenate all the
#   averaged series into a single DataFrame to insert into the SQL database.

#   Group by ZIP + HCPCS.
grouped = data.groupby(['Rndrng_Prvdr_Zip5', 'HCPCS_Cd'])

#   Aggregate all the means of the payments per procedure per zip code.
zip_stats_df = grouped.agg({
    'Avg_Sbmtd_Chrg': 'mean',
    'Avg_Mdcr_Alowd_Amt': 'mean',
    'Avg_Mdcr_Pymt_Amt': 'mean',
    'Estimated_Average_Payment': 'mean',
    'Rndrng_NPI': pd.Series.nunique
}).reset_index()
zip_stats_df.rename(columns={'Rndrng_NPI': 'provider_count'}, inplace=True)

#   Checking if there are in fact multiple providers who offer a procedure per zip code.
print(zip_stats_df[zip_stats_df['provider_count'] >= 3])


#   Now, we've grouped zip codes with respective HCPCS codes. We can add this to the PostgreSQL database.
conn_string = 'postgresql://postgres:hidden@localhost:5432/postgres'

db = create_engine(conn_string)
conn = db.connect()

zip_stats_df.to_sql('zip_stats_df', con=conn, if_exists='replace', index=False)
conn = psycopg2.connect(conn_string)
conn.autocommit = True
cursor = conn.cursor()

sql1 = '''select * from zip_stats_df;'''
cursor.execute(sql1)
for i in cursor.fetchall():
    print(i)

# conn.commit()
conn.close()