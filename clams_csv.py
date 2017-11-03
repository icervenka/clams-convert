#!/usr/bin/env python3

import pandas as pd
import numpy as np
import re
import glob
import sys
import datetime
import argparse
import os

wd = os.getcwd()
input_filepath = wd
output_filepath = wd

parser = argparse.ArgumentParser(description = "Concatenate CSV files from CLAMS to one file for analysis by clams-vis")

parser.add_argument('-i', '--input', help = "Path to input files")
parser.add_argument('-o', '--output', help = "Path where the output will be stored")
args = parser.parse_args()

if args.input:
    input_filepath = args.input

if args.output:
    output_filepath = args.output

#input_filepath = "/Users/igor/Google Drive/programming/clams_csv/jorge/"
#output_filepath = "/Users/igor/Google Drive/programming/clams_csv/jorge/"
#print(input_filepath)

file_list = []
data_frame_final = pd.DataFrame()

for file in glob.glob(str(input_filepath)+"/*.CSV"):
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
    
    text = []
    
    with open(csv, "r") as file:
        text = file.read().splitlines()
        text = [line for line in text if len(line) > 0]
        
        for i, line in enumerate(text):
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
    
    records = [x.split(',') for x in text]
    data_records = np.array(records[start_data_line:end_data_line])
    
    
    data = pd.DataFrame(data_records)
    data = data.iloc[:,use_columns]
    data.columns = column_names
    
    cols = data.columns.tolist() 
    cols = cols[:2]+cols[-1:]+cols[2:-1]
    data = data[cols]
    
    data.iloc[:,2].replace("ON", "Light", inplace = True)
    data.iloc[:,2].replace("OFF", "Dark", inplace = True)
    
    data.insert(loc = 0, column = "Subject", value = subject_id)
    data.insert(loc = len(data.columns), column = "Event Log", value = "")
    
    event_records = np.array(records[events_line:])
    
    events = pd.DataFrame(event_records[:,[0,3]])
    events.columns = ["Interval", "Event Log"]
    events.Interval = events.Interval.apply(pd.to_numeric)
    events = events.set_index("Interval")
    
    data.iloc[list(events.index), [-1]] = events["Event Log"]

    #TODO set float precision
    
    if data_frame_final.empty:
        data_frame_final = data
    else:
        data_frame_final = data_frame_final.append(data)
        
print("Processing complete")

filename = str(datetime.date.today())+"_result_all.csv"
with open(str(output_filepath)+filename, "w") as file:
    data_frame_final.to_csv(file, index = False)
  
print("\n")
print("Results were saved to file: "+filename)
