#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 16:24:03 2019

@author: igocer
"""

import re

# columns specification file
column_specs_file = "clams_column_specification.txt"

# RE patterns to identify lines of interest in csv data
patterns_classic = {
    "file" : re.compile("Oxymax CSV File"),
    "subject" : re.compile("Subject ID"),
    "data_start" : re.compile(":DATA"),
    "events" : re.compile(":EVENTS"),
    "start_offset" : 2,
    "events_offset" : 2
}

patterns_tse = {
    "start" : re.compile("Date,Time")
}