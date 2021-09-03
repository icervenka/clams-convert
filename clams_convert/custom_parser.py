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
