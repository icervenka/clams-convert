#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 16:24:03 2019

@author: igocer
"""

import pandas as pd
import re
import glob
import sys
import datetime
import argparse
import os

# import various CLAMS system specific constants and variables
import clams_const


def parse_classic(csv, column_specs):

    col_mapper = column_specs.set_index('name_classic').loc[:,'name_app'].dropna().to_dict()
    col_finder = column_specs.dropna().set_index('name_app').loc[:,'name_classic'].to_dict()

    file_type = False
    subject_id = None
    start_data_line = None
    end_data_line = None
#    events_line = None

    # determine the line locations for RE patterns of interest and set corresponding line numbers
    with open(csv, "r") as file:
        #TODO there is an error due to utf failing to read degree sign
        text = file.read().splitlines()
        # in the original exported files, every second line is empty
        # remove empty lines from csv file so it will match read_csv command
        text = [line for line in text if len(line) > 0]

        for i, line in enumerate(text):
            if re.match(clams_const.patterns_classic['file'], line):
                file_type = True
            if re.match(clams_const.patterns_classic['subject'], line):
                subject_id = line
            if re.match(clams_const.patterns_classic['data_start'], line):
                start_data_line = i+clams_const.patterns_classic['start_offset']
#            if re.match(clams_const.patterns_classic['events'], line):
#                events_line = i+clams_const.patterns_classic['events_offset']
#                end_data_line = i-1

    # skip if csv file is a parameter file not an animal file
    if file_type == False:
        print("Skipping: "+csv+" - Not an animal data file")
        return(pd.DataFrame())
    else:
        print("Processing: "+csv)

    # parse subject ID
    # check the number of parsed subject IDs
    # it often happens that they haven't been set at the beginning of experiment
    # solution is either to infer them or let the file owner enter them manually
    subject_id = subject_id.strip().split(',')[1]
    if(len(subject_id) < 1):
        print("Subject ID is not present in the animal csv file.")
        sys.exit("Please enter the Subject IDs manually and try again.")

    # parse data from the csv file
    # complicated statement due to the weird structure of the original format
    data = pd.read_csv(csv,
                       header = [start_data_line, start_data_line+1, start_data_line+2],
                       na_values = '-',
                       nrows = end_data_line - start_data_line - clams_const.patterns_classic['start_offset'])
    data.columns = data.columns.droplevel([1,2])
    data = data.filter(column_specs.name_classic.dropna())

    # reformat subject ID column
    data[col_finder['subject']] = subject_id

    # find a better way of doing this, possibly with col_finder
    date_time_colstring = "DATE/TIME"

    # match the date/time format of shiny_app
    data.loc[:,date_time_colstring] = pd.to_datetime(data.loc[:, date_time_colstring], infer_datetime_format = True)
    data.loc[:,date_time_colstring] = data.loc[:,date_time_colstring].dt.strftime("%d-%m-%Y %H:%M:%S")
    data.loc[:,date_time_colstring] = data.loc[:,date_time_colstring].astype(str)

    # rename Light/Dark phase to match shinyapps utility specification
    data.loc[:,col_finder['light']] = data.loc[:,col_finder['light']].apply(lambda x: 1 if x == "ON" else 0)

    # set float precision for heat and rer values
    # when read from csv they are jumbled due to imprecise float precision
    data[col_finder['heat']] = data[col_finder['heat']].round(6)
    data[col_finder['rer']] = data[col_finder['rer']].round(6)

    data['xyt'] = data[col_finder['xt']] + data[col_finder['yt']]
    data['xf'] = data[col_finder['xt']] + data[col_finder['xa']]
    data['yf'] = data[col_finder['yt']] + data[col_finder['ya']]

    # events have been disabled for now, they don't seem to bring anything useful
    # insert and Event Log column and initialize with empty string
#    data['events'] = ""
#
#    # parse events from csv file
#    events = pd.read_csv(csv,
#                         header = [events_line, events_line+1])
#    events.columns = events.columns.droplevel([1])
#    events = events.filter(["INTERVAL","DESC."])
#    events.columns = ["interval", "events"]
#
#    if not events.empty:
#        events = events.set_index("interval")
#        data.iloc[list(events.index), [-1]] = events["events"]

    # unify and reorder columns according to common specs
    data = data.rename(columns=col_mapper)
    data = data[column_specs.name_app.dropna()]

    return(data)


def parse_tse(csv, column_specs):

    col_mapper = column_specs.set_index('name_tse').loc[:,'name_app'].dropna().to_dict()
    col_finder = column_specs.dropna().set_index('name_app').loc[:,'name_tse'].to_dict()

    tse_start_line = None

    with open(csv, "r") as file:
        text = file.read().splitlines()
        text = [line for line in text if len(line) > 0]

        # In case there is animal info in the beginning
        for i, line in enumerate(text):
            if re.match(clams_const.patterns_tse['start'], line):
                tse_start_line = i

    if tse_start_line == None:
        sys.exit("TSE format not recognized. Please make sure that it contains the header with following string: "+ clams_const.patterns_tse['start'])

    data = pd.read_csv(csv, skiprows = [1], header = tse_start_line, na_values = '-', usecols = column_specs.name_tse.dropna())
    data = data.interpolate(axis = 0)

    data["date_time"] = data["Date"] + " " + data["Time"]

    # Decide on the datetime format  when updating the shinyapps
    data.loc[:,"date_time"] = pd.to_datetime(data.loc[:, "date_time"], infer_datetime_format = True)
    data.loc[:,"date_time"] = data.loc[:,"date_time"].dt.strftime("%d-%m-%Y %H:%M:%S")
    data.loc[:,"date_time"] = data.loc[:,"date_time"].astype(str)

    data.loc[:,col_finder['light']] = data.loc[:,col_finder['light']].apply(lambda x: 1 if x > 50 else 0)

    # feed and drink are reported as cumulative in the tse, have to be changed to interval
    # otherwise the aggregation will not work
    data[col_finder['feed']] = data[col_finder['feed']].diff().round(6)
    data[col_finder['drink']] = data[col_finder['drink']].diff().round(6)

    # set float precision for heat and rer values
    # when read from csv they are jumbled due to imprecise float precision
    data[col_finder['heat']] = data[col_finder['heat']].round(6)
    data[col_finder['rer']] = data[col_finder['rer']].round(6)

    ser = []
    interval_count = list(data.groupby(col_finder['subject']).count()["date_time"])
    for x in interval_count:
        ser = ser + list(range(0,x))

    data.insert(1, "interval", ser)

    # events have been disabled for now, they don't seem to bring anything useful
    # first interval doesn't have measurement for VO2 and VCO2 and can't be interpolated so it's removed
    data = data[data["interval"] != 0]

    # insert and Event Log column and initialize with empty string
#    data["events"] = ""

    # unify and reorder columns according to common specs
    data = data.rename(columns=col_mapper)
    data = data[column_specs.name_app.dropna()]

    return(data)


def main():

    ##### set up variables and parse commandline arguments

    # set default directory to current working directory
    wd = os.getcwd()
    input_filepath = wd
    output_filepath = wd

    # parse input and output arguments arguments
    parser = argparse.ArgumentParser(description = "Concatenate CSV files from CLAMS to one file for analysis by clams-vis")

    parser.add_argument('-i', '--input', help = "Path to input files")
    parser.add_argument('-o', '--output', help = "Path where the output will be stored")
    parser.add_argument('-s', '--system', help = "System which was used to record the data", choices = ("classic", "tse"), default = "classic")
    parser.add_argument('-d', '--date', help = "Date-Time format used for export. Accepts strftime format strings", default = "%d-%m-%Y %H:%M:%S")
    args = parser.parse_args()

    if args.input:
        input_filepath = args.input

    if args.output:
        output_filepath = args.output

    table_format = args.system

    # array to be populated with list of files
    file_list = []

    # find csv files to process or exit
    for file in glob.glob(str(input_filepath)+"/*.[Cc][Ss][Vv]"):
        file_list.append(file)

    if len(file_list) == 0:
        sys.exit("No CSV files to process")

    # check if the column specification file for different systems exists and import it
    if os.path.exists(clams_const.column_specs_file):
        column_specs = pd.read_csv(clams_const.column_specs_file, sep = '\t')
    else:
        sys.exit("No specification file for columns from CLAMS system was found")

    ##### process data based on selected system

    if table_format == "classic":
        # data frame to be appended with individual animal data
        data_frame_final = pd.DataFrame()
        
        for csv in file_list:
            data = parse_classic(csv, column_specs)

            # Skipping for iteration if the file is not the correct csv format
            if(data.empty):
                continue

            # append individual processed files to final data frame
            if data_frame_final.empty:
                data_frame_final = data
            else:
                data_frame_final = data_frame_final.append(data)


    if table_format == "tse":
        if len(file_list) > 1:
            sys.exit("Too many csv files to process. For TSE system type only one csv file has to be present in the directory.")

        csv = file_list[0]
        data_frame_final = parse_tse(csv, column_specs)


    ##### data export

    print("Processing complete")
    filename = str(datetime.date.today())+"_"+table_format+".csv"
    with open(str(output_filepath)+"/"+filename, "w") as file:
        data_frame_final.to_csv(file, index = False)
    print("\n")
    print("Results were saved to file: "+filename)


if __name__ == "__main__":
    main()
