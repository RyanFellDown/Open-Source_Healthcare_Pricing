import pandas as pd
import json

code_keys_description = {}
abbrev_map = {}
allLines = []
allCodes = []


#   First, take all the lines from the codes file and put the descriptions in a list.
#   Also, make a list of allLines (descriptions) and allCodes (codes + lines).
with open("data/raw/2023_DHS_Code_List_Addendum_12_01_2022.txt", 'r') as file:
    lines = file.readlines()
    for line in lines:
        line = line.split("\t", 2)
        
        allLines.append(line[1])
        allCodes.append([line[0], line[1]])


#   Second, write all these descriptions into a TXT file. I used ChatGPT to produce
#   a mapping of all the abbreviated words in the descriptions to their full words.
with open("data/processed/abbreviations.txt", 'wt') as file:
    for line in allLines:
        line = line + "\n"
        file.write(line)        


#   Third, load that mapping in as a dictionary.
with open("data/processed/abbreviation_mappings.json") as file:
    abbrev_map = json.load(file)


#   Fourth, go through every line in the allLines array, change any words that match
#   within the description, and then add the code + new description to a mapping
#   (which I will save as a new and final JSON file.)
for code in allCodes:
    split = code[1].split(" ")
    newDescription = ""
    for word in split:
        if word in abbrev_map:
            newDescription = newDescription + " " + abbrev_map[word]
        else:
            newDescription = newDescription + " " + word
    code_keys_description[code[0]] = newDescription

print(code_keys_description)

#   Fifth and finally, dump the dictionary into a JSON file.
with open("data/processed/final_code_mappings.json", 'w') as file:
    json.dump(code_keys_description, file)