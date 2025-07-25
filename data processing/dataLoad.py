import pandas as pd


#Currently ~9 million rows, takes a long time and strong processing to do this, so only doing around like 100 right now.
#We're going to keep the Zip Code, visit code, the amount they paid, and the amount the amount medicare wanted to be paid.
data = pd.read_csv("data/raw/Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2022.csv", usecols=["Rndrng_Prvdr_Zip5", "HCPCS_Cd", "HCPCS_Desc", "Tot_Benes", "Tot_Srvcs", "Avg_Sbmtd_Chrg", "Avg_Mdcr_Alowd_Amt", "Avg_Mdcr_Pymt_Amt"], nrows=200)
hcpcsCodes = pd.read_csv("data/processed/HCPS Codes - Sheet1.csv")
data["Estimated_Average_Payment"] = data["Avg_Mdcr_Alowd_Amt"] - data["Avg_Mdcr_Pymt_Amt"]
print(data.head(n=10))


#Add the codes to a hashmap and mark with 1 to note they're available. If not found, remove said row.
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
#   submitted, medicare allowed, medicare payment, and patient payment per zip code.
submitted = data.groupby('Rndrng_Prvdr_Zip5')['Avg_Sbmtd_Chrg'].mean()
mdcrAllowed = data.groupby('Rndrng_Prvdr_Zip5')['Avg_Mdcr_Alowd_Amt'].mean()
mdcrPaid = data.groupby('Rndrng_Prvdr_Zip5')['Avg_Mdcr_Pymt_Amt'].mean()
patientPaid = data.groupby('Rndrng_Prvdr_Zip5')['Estimated_Average_Payment'].mean()
print("Average paid amount per this zip code is:\n", patientPaid)