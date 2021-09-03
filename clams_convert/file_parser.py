import re
import pandas as pd
import numpy as np
from abc import abstractmethod
from . import errors as e

def check_subject_names(names):
    import string
    letters = iter(string.ascii_letters)
    seen = set()
    renamed = []
    for n in names:
        while n in seen:
            append = next(letters)
            print("Renaming duplicate subjects: {} -> {}".format(n, n+append))
            n = n + append
        seen.add(n)
        renamed.append(n)
    return renamed

def convert_values(df, conversion_factor, columns="all"):
    if isinstance(df, pd.Series):
        df = df * conversion_factor
    else:
        if columns == "all":
            columns = df.columns
        df = df.loc[:, columns].apply(lambda x: x * conversion_factor, axis=1)
    return df

class FileParser:

    def __init__(self, time_fmt_in, mapper=None, repair_header=True):
        self.time_fmt_in = time_fmt_in
        self.time_fmt_out = "%Y-%m-%d %H:%M:%S"
        self.mapper = mapper
        self.patterns = {
            "file_type": None,
            "subject": None,
            "data_start": None,
            "data_end": ""
        }
        self.offsets = {
            "header_start": 0,
            "data_start": 0,
            "data_end": 0
        }
        # self.format_description = {
        #     "multifile": None,
        #     "multiparameter": None
        # }
        self.patterns_other = {}
        self.line_numbers = {}
        self.split_char = ","
        self.conversion_factor = 1
        self.interval = None

    @abstractmethod
    def parse_subject_names(self, text):
        pass

    @abstractmethod
    def prettify(self, data, subjects, *args):
        pass

    @staticmethod
    def parse_text(file):
        text = file.read().splitlines()
        text = [line for line in text if len(line) > 0]
        return text

    @staticmethod
    def search_pattern(pattern, text):
        # searches for the last occurring pattern
        # might be changed to discard all header records from data
        line_no = 0
        for i, line in enumerate(text):
            if re.search(pattern, line):
                line_no = i
        return line_no

    def update_info(self, **kwargs):
        for k, v in kwargs.items():
            try:
                self.__dict__[k].update(v)
            except AttributeError:
                print("Trying to modify non-existent property in FileParser.")


    def init_line_numbers(self, text):
        self.is_set_patterns()
        for k, v in self.patterns.items():
            self.line_numbers[k] = self.search_pattern(v, text)
        self.validate_line_numbers()
        return self

    def is_set_patterns(self):
        for k, v in self.patterns.items():
            if v is None:
                raise e.FileFormatError("Set re for " + str(k) + " is required for a custom parser.")

    def validate_line_numbers(self):
        for k, v in self.line_numbers.items():
            if v is None:
                raise e.FileFormatError("Format not recognized, required pattern "
                                        + "'{}' for {} is missing. ".format(self.patterns[k].pattern, k))
        return self

    def make_header_unique(self, header):
        if len(set(header)) != len(header):
            if self.repair_header == True:
                # adds unique prefixes to header in case the column names are the same
                return ["X" + str(x) for x in range(len(header))]
            else:
                raise e.HeaderNotUniqueError("Column names in data file are not unique.")
        else:
            return header
    def read_data(self, text):
        header = text[self.line_numbers['data_start'] + self.offsets['header_start']]
        header = header.split(self.split_char)

        df_start = self.line_numbers['data_start'] + self.offsets['data_start']
        df_end = max(len(text), self.line_numbers['data_end'] + self.offsets['data_end'])

        records = text[df_start:df_end]
        split_records = [x.split(self.split_char) for x in records]
        data = pd.DataFrame(np.array(split_records))
        data = data.iloc[:, 0:len(header)]
        data.columns = self.make_header_unique(header)
        return data

    def format_ts(self, ts):
        ts = ts.str.strip()
        ts = pd.to_datetime(ts, format=self.time_fmt_in)
        ts = ts.dt.strftime(self.time_fmt_out)
        ts.name = "date_time"
        return ts

    def parse(self, file):
        # TODO there is an error in clams-tse due to utf failing to read degree sign
        with open(file, "r") as current_file:
            text = self.parse_text(current_file)
            try:
                self.init_line_numbers(text)
                subjects = self.parse_subject_names(text)
                data = self.read_data(text)
                data = self.prettify(data, subjects)
            except (e.FileFormatError, e.SubjectIdError) as err:
                print(file, " - ", err, "Skipping")
                raise
            else:
                return data

    def __str__(self):
        print(vars(self))
