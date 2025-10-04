from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os


#Currently ~9 million rows, takes a long time and strong processing to do this, so only doing around like 200 right now.
#We're going to keep the Zip Code, visit code, the amount they paid, and the amount the amount medicare wanted to be paid.
baseDirectory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
cmsData = os.path.join(baseDirectory, 'data', 'raw', 'Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2022.csv')
hcpcsCodesData = os.path.join(baseDirectory, 'data', 'processed', 'HCPS Codes - Sheet1.csv')


data = pd.read_csv(cmsData, usecols=["Rndrng_NPI", "Rndrng_Prvdr_Zip5", "HCPCS_Cd", "HCPCS_Desc", "Tot_Benes", "Tot_Srvcs", "Avg_Sbmtd_Chrg", "Avg_Mdcr_Alowd_Amt", "Avg_Mdcr_Pymt_Amt"])
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


#2. Checking if any Zip codes are incorrect format (either 99999 or 00000)
#   Check if the code in the data is one of the defined HCPCS Codes listed. If not, remove.
#   Will continue to expand the number of codes available, otherwise this is a limiting factor.

#   Convert all numpy.ints to strings
data['Rndrng_Prvdr_Zip5'] = data['Rndrng_Prvdr_Zip5'].astype(str).str.strip()
data = data.dropna(subset=['Rndrng_Prvdr_Zip5','HCPCS_Cd'])

#   Drop if the zip codes are 99999 or 00000, fake codes, and add leading zeroes if it's shorter than 5 digits.
data = data[data["Rndrng_Prvdr_Zip5"] != "99999"]
data = data[data["Rndrng_Prvdr_Zip5"] != "000000"]
data['Rndrng_Prvdr_Zip5'] = data['Rndrng_Prvdr_Zip5'].str.zfill(5)

#   Drop if it's NOT an HCPCS code, which are pulled from a separate CSV (hence why we need to filter as such).
data = data[~data['HCPCS_Cd'].isin(hcpcsHash)]
print(data.head)



#3. Group all the charges by their zip codes and then return the average of each one, giving us the the average
#   submitted, medicare allowed, medicare payment, and patient payment per zip code. Then, concatenate all the
#   averaged series into a single DataFrame to insert into the SQL database.

#   Group by ZIP + HCPCS.
grouped = data.groupby(['Rndrng_Prvdr_Zip5', 'HCPCS_Cd'])
print("number of groups:", grouped.ngroups)

#   Aggregate all the means of the payments per procedure per zip code.
zip_stats_df = grouped.agg({
    'Avg_Sbmtd_Chrg': 'mean',
    'Avg_Mdcr_Alowd_Amt': 'mean',
    'Avg_Mdcr_Pymt_Amt': 'mean',
    'Estimated_Average_Payment': 'mean',
    'Rndrng_NPI': pd.Series.nunique
}).reset_index()

print("ZIP_DF Check 1: ", zip_stats_df)

zip_stats_df.rename(columns={'Rndrng_NPI': 'provider_count'}, inplace=True)

#   Checking if there are in fact multiple providers who offer a procedure per zip code.
#   Returns an Empty DF if none of the provider counts are greater than three.
print("ZIP_DF Check 2: ", zip_stats_df[zip_stats_df['provider_count'] >= 3])


password = input("Enter you password here.")

#   Now, we've grouped zip codes with respective HCPCS codes. We can add this to the PostgreSQL database.
conn_string = f'postgresql://postgres:{password}@localhost:5432/postgres'

db = create_engine(conn_string)
conn = db.connect()
print("Connection 1 worked.")

zip_stats_df.to_sql('zip_stats_df', con=conn, if_exists='replace', index=False)
conn = psycopg2.connect(conn_string)
conn.autocommit = True
cursor = conn.cursor()
print("Connection 2 and data to DF woked.")

sql1 = '''select * from zip_stats_df;'''
cursor.execute(sql1)
for i in cursor.fetchall():
    print(i)

conn.commit()
conn.close()
