import pandas as pd
import re
import glob
import sys
import datetime

file_list = []
data_frame_final = pd.DataFrame()

for file in glob.glob("animal_data/*.CSV"):
    file_list.append(file)

if len(file_list) == 0:
    sys.exit("No CSV files to process")

column_names = ["Interval", "Date/Time", "Volume O2", "O2 In",	"O2 Out","Delta O2", "Accumulated O2",
                "Volume CO2", "CO2 In", "CO2 Out", "Delta CO2",	"Accumulated CO2", "RER", "Heat", "Flow",	
                "Feed Weight 1", "Feed Acc. 1", "Drink Weight 1",	"Drink Acc. 1", "X Total", "X Ambulatory", "Y Total",	
                "Y Ambulatory", "Z Total", "Light/Dark"]
use_columns = [0]+list(range(2,16))+list(range(17,26))+[31]

subject_id_pattern = re.compile("Subject ID")
start_data_pattern = re.compile(":DATA")
csv_file_pattern = re.compile("Oxymax CSV File")
events_pattern = re.compile(":EVENTS")

for csv in file_list:
    
    subject_id = None
    start_data_line = None
    end_data_line = None
    csv_file_type = False
    
    events_line = None
    events = {}
    
    with open(csv, "r") as file:
        for i, line in enumerate(file):
            if re.match(csv_file_pattern, line):
                csv_file_type = True
            if re.match(subject_id_pattern, line):
                subject_id = line
            if re.match(start_data_pattern, line):
                start_data_line = i+5
            if re.match(events_pattern, line):
                events_line = i+4
                end_data_line = i-1
      
    if csv_file_type == False:
        print("Skipping: "+csv+" - Not an animal data file")
        continue
    else: 
        print("Processing: "+csv)
    
    subject_id = subject_id.strip().split(',')[1]
    
              
    with open(csv, "r") as file:       
        data = pd.read_csv(file, header = None,  names = column_names, usecols = use_columns, skiprows = start_data_line, nrows = end_data_line - start_data_line)
    
    cols = data.columns.tolist() 
    cols = cols[:2]+cols[-1:]+cols[2:-1]
    data = data[cols]
    data.iloc[:,2].replace("ON", "Light", inplace = True)
    data.iloc[:,2].replace("OFF", "Dark", inplace = True)
    
    data.insert(loc = 0, column = "Subject", value = subject_id)
    data.insert(loc = len(data.columns), column = "Event Log", value = "")
    
    with open(csv, "r") as file:       
        events = pd.read_csv(file, header = None, skiprows = events_line, usecols = [0,3], names = ["Interval", "Description"])
    
    data.iloc[list(events["Interval"]), [-1]] = list(events["Description"])

    
    #TODO set float precision
    
    if data_frame_final.empty:
        data_frame_final = data
    else:
        data_frame_final = data_frame_final.append(data)
        
print("Processing complete")

filename = str(datetime.date.today())+"_result_all.csv"
with open(filename, "w") as file:
    data_frame_final.to_csv(file, index = False)
  
print("\n")
print("Results were saved to file: "+filename)