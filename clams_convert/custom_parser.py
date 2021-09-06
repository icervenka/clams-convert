from .file_parser import FileParser
from .file_parser import check_subject_names, convert_values
from . import errors as e
import pandas as pd
import numpy as np
import re

class AnalysisVisParser(FileParser):

    def __init__(self, *args):
        super().__init__("%Y-%m-%d %H:%M:%S", *args)
        patterns = {
            "file_type": "analysis-vis",
            "subject": "\[Data\]",
            "data_start": "\[Data\]",
        }
        offsets = {
            "header_start": 1,
            "data_start": 2
        }
        # TODO this has to inherit from the original parser
        # format_description = {
        #     "multifile": False,
        #     "multiparameter": True
        # }
        # self.update_info(**dict(patterns=patterns, offsets=offsets, format_description=format_description))
        self.update_info(**dict(patterns=patterns, offsets=offsets))

    def parse_subject_names(self, text):
        pass

    def prettify(self, data, *args):
        # when parsed from records, types are not set correctly
        # data = data.infer_objects()
        data.date_time = pd.to_datetime(data.date_time)
        data.interval = pd.to_numeric(data.interval)
        desc = ["subject", "date_time", "interval"]
        data = pd.concat([data.loc[:, desc], data.drop(columns=desc).apply(pd.to_numeric)], axis=1)
        return data

class ClamsOxymaxParser(FileParser):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        patterns = {
            "file_type": re.compile("Oxymax CSV File"),
            "subject": re.compile("Subject ID"),
            "data_start": re.compile(":DATA"),
            "cage": re.compile("Group/Cage"),
            "data_end": re.compile(":EVENTS")
        }
        offsets = {
            "header_start": 2,
            "data_start": 5,
            "data_end": 0
        }
        # format_description = {
        #     "multifile": True,
        #     "multiparameter": True
        # }
        # self.update_info(**dict(patterns=patterns, offsets=offsets, format_description=format_description))
        self.update_info(**dict(patterns=patterns, offsets=offsets))

    def parse_subject_names(self, text):
        subject_id = text[self.line_numbers['subject']]
        subject_id = subject_id.split(',')[1].strip()
        if len(subject_id) < 1:
            raise e.SubjectIdError("Subject ID is not present in the animal csv file.")
        return subject_id

    def prettify(self, data, subjects, *args):
        colfind = self.mapper.find
        data = data.filter(self.mapper.specs.colnames.dropna())
        data = data.astype(self.mapper.typer)

        # reformat subject ID column
        data['subject'] = subjects

        # match the date/time format of shiny_app
        data.loc[:, colfind("date_time")] = self.format_ts(data.loc[:, colfind("date_time")])
        data.loc[:, colfind('light')] = data.loc[:, colfind('light')].apply(lambda x: 1 if x == "ON" else 0)

        # set float precision for heat and rer values
        # when read from csv they are jumbled due to imprecise float precision
        for param in ['feed', 'drink', 'heat', 'rer']:
            data[colfind(param)] = data[colfind(param)].round(6)

        data['xyt'] = data[colfind('xt')] + data[colfind('yt')]
        data['xf'] = data[colfind('xt')] - data[colfind('xa')]
        data['yf'] = data[colfind('yt')] - data[colfind('ya')]

        data = data.rename(columns=self.mapper.mapper)
        data = data[self.mapper.specs.app.dropna()]

        return data
