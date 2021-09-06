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


class ClamsTseParser(FileParser):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        patterns = {
            "file_type": re.compile("Date,Time"),
            "subject": re.compile(""),
            "data_start": re.compile("Date,Time"),
        }
        offsets = {
            "header_start": 0,
            "data_start": 2,
            "data_end": 0
        }
        # format_description = {
        #     "multifile": False,
        #     "multiparameter": True
        # }
        # self.update_info(**dict(patterns=patterns, offsets=offsets, format_description=format_description))
        self.update_info(**dict(patterns=patterns, offsets=offsets))

    def parse_subject_names(self, text):
        pass

    def prettify(self, data, *args):
        colfind = self.mapper.find

        data.replace('-', np.NaN, inplace=True)
        data = data.astype(self.mapper.typer)

        # feed and drink are reported as cumulative in the tse, have to be changed to interval
        # otherwise the aggregation will not work
        data[colfind('feed')] = data[colfind('feed')].diff()
        data[colfind('drink')] = data[colfind('drink')].diff()

        data = data.interpolate(axis=0)
        # TODO this has to be moved
        data.drop(data.index[:1], inplace=True)

        data["DateTime"] = data["Date"] + " " + data["Time"]
        self.mapper.update({"display": "Date-Time",
                            "app": "date_time",
                            "unit": None,
                            "aggregate": None,
                            "colnames": "DateTime"}, 0)
        data.loc[:, colfind("date_time")] = self.format_ts(data.loc[:, colfind("date_time")])
        data.loc[:, colfind('light')] = data.loc[:, colfind('light')].apply(lambda y: 1 if y > 50 else 0)

        # set float precision for heat and rer values
        # when read from csv they are jumbled due to imprecise float precision
        for param in ['feed', 'drink', 'heat', 'rer']:
            data[colfind(param)] = data[colfind(param)].round(6)

        ser = []
        interval_count = list(data.groupby(colfind('subject')).count()[colfind("date_time")])
        for x in interval_count:
            ser = ser + list(range(0, x))
        data.insert(1, "interval", ser)
        self.mapper.update({"display": "Interval",
                            "app": "interval",
                            "unit": None,
                            "aggregate": None,
                            "colnames": "DateTime"}, 0)

        # events have been disabled for now, they don't seem to bring anything useful
        # insert and Event Log column and initialize with empty string
        #    data["events"] = ""

        # unify and reorder columns according to common specs
        data = data.rename(columns=self.mapper.mapper)
        data = data[self.mapper.specs.app.dropna()]

        return data

class FwrZierathOldParser(FileParser):

    def __init__(self, *args):
        super().__init__(*args)
        patterns = {
            "file_type": re.compile('Turns Data'),
            "subject": re.compile('Turns Data'),
            "data_start": re.compile('Turns Data')
        }
        offsets = {
            "data_start": 1
        }
        # format_description = {
        #     "multifile": False,
        #     "multiparameter": False
        # }
        # self.update_info(**dict(patterns=patterns, offsets=offsets, format_description=format_description))
        self.update_info(**dict(patterns=patterns, offsets=offsets))

        self.turns_conversion_factor = 0.6912

    def parse_subject_names(self, text):
        subject_ids = text[self.line_numbers['subject']]
        subject_ids = subject_ids.split(',')
        subject_ids = filter(lambda x: self.patterns['subject'].pattern in x, subject_ids)
        subject_ids = [x.split(' ')[0] for x in subject_ids]
        return subject_ids

    def prettify(self, data, subjects, *args):
        df_list = []
        for subject in subjects:
            data_sub = data.filter(regex=subject, axis=1)
            date = data_sub.filter(regex='Turns Date').iloc[:, 0]
            time = data_sub.filter(regex='Turns Time').iloc[:, 0]
            turns = data_sub.filter(regex='Turns Data').iloc[:, 0]
            turns.name = 'distance'
            turns = turns.astype(float)
            turns = convert_values(turns, self.turns_conversion_factor)
            date_time = date + " " + time
            date_time = self.format_ts(date_time)
            data_subject_concat = pd.concat([pd.Series([subject]*len(date_time), name='subject'),
                                             date_time,
                                             pd.Series(range(len(date_time)), name='interval'),
                                             turns],
                                            axis=1)
            # data_subject_concat.columns = ["subject", "date_time", "interval", "distance"]
            df_list.append(data_subject_concat)

        df = pd.concat(df_list, axis=0)
        return df
