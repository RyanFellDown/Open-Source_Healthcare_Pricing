import pandas as pd
import re


expanded_content = [expand_line(line.strip(), abbrev_map) + "\n" for line in content]

with open("2023_DHS_Code_List_Expanded_Fixed.txt", "w", encoding="utf-8") as f:
    f.writelines(expanded_content)


with open("data/raw/2023_DHS_Code_List_Expanded.txt", 'r') as file:
    lines = file.readlines()
    
    splitText = re.split(r'\n{5}', lines[0])
    
    print(splitText, len(splitText))