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

class FwrZierathParser(FileParser):

    def __init__(self, *args):
        super().__init__(*args)
        patterns = {
            "file_type": "Channel Name",
            "subject": "Channel Group",
            "data_start": "Sensor Type",
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
        subjects = ','.join(text[self.line_numbers['subject']:self.line_numbers['data_start']]).split(',')[1:]
        subjects = [re.sub(" ", "_", x) for x in subjects]
        return subjects

    def prettify(self, data, subjects, *args):
        subjects = check_subject_names(subjects)
        date_time = data.iloc[:, 0]
        date_time = self.format_ts(date_time)

        turns_only = data.iloc[:, 1:len(subjects) + 1]
        turns_only.columns = subjects
        turns_only = turns_only.astype(float)
        turns_only = FileParser.convert_values(turns_only, self.turns_conversion_factor)

        df = pd.concat([date_time, pd.Series(range(len(date_time)), name='interval'), turns_only], axis=1)
        df = df.melt(id_vars=["date_time", "interval"], var_name="subject", value_name="distance")
        df = df[["subject", "date_time", "interval", "distance"]]

        return df

class FwrCanlonParser(FileParser):

    def __init__(self, *args):
        super().__init__(*args)
        self.wheel_type = "canlon"
        self.start_data = start_date

        if (not is_date_8(start_date)):
            while (not is_date_8(inp)):
                inp = input("Date when experiment started (yyyymmdd): ")
                inp = "20170908"

        self.init_time = datetime.strptime(inp, "%Y%m%d")
        self.split_char = '\t'
        self.turns_conversion_factor = 0.6912
        self.wide_format = 0

        self.patterns = {
            "file_type": None,
            "subject": None,
            "data_start": None,
            "data_end": None
        }
        self.offsets = {
            "data_start": 0
        }

    def parse_subject_names(self, text):
        subject = os.path.splitext(os.path.basename(file))[0]
        return subject

    def parse_data(self, file):
        #        files = self.load_files(directory)
        #        subject = ""
        #
        #        for f in files:
        #            with open(f, "r") as current_file:
        #                text = self.parse_text(current_file)
        #                self.init_line_numbers(text)
        #                subject = self.parse_subject_names(f)
        #                self.subjects.append(subject)
        #                records = self.parse_records(text, self.start_line, self.end_line)
        #                data = self.records_to_df(records, self.split_char)
        #                data[self.subject_string] = subject
        #
        #                yield(data)
        pass

    def prettify(self, data, *args):
        data = data.iloc[:, (3, 0, 1)]

        data.columns = [self.subject_string, self.datetime_string, self.distance_string]

        data.date_time = data.date_time.astype(int)
        interval = data.date_time[1] - data.date_time[0]

        data.Distance = data.Distance * interval
        data = data[data.Distance != 'NaN']
        data = data[data.Distance != '']

        data.Date = data.Date.apply(lambda x: self.init_time + timedelta(minutes=x))
        data.Distance = data.Distance.astype(int)
        data.reset_index(inplace=True, drop=True)
        return data


class FwrWesterbladParser(FileParser):

    def __init__(self, *args):
        super().__init__(*args)
        self.wheel_type = "westerblad"
        self.patterns['start_time'] = 'Start Time'
        self.patterns['end_time'] = 'End Time'
        self.patterns['wheels'] = 'Wheels'
        self.patterns['bin_size'] = 'Bin Size'
        self.patterns['activity_units'] = 'Activity Units'
        self.patterns['bin'] = 'Bin'
        self.split_char = "\t"
        self.start_line_pattern = 'bin'
        self.turns_conversion_factor = 1000

        self.patterns = {
            "file_type": None,
            "subject": None,
            "data_start": None,
            "data_end": None
        }
        self.offsets = {
            "data_start": 0
        }

    def parse_subject_names(self, text):
        subjects = text[self.line_numbers['bin']].split(self.split_char)[1:]
        subjects = ["s" + x.split(' ')[2] for x in subjects]
        return subjects

    def parse_data(self, file):
        pass

    def prettify(self, data, *args):
        subjects = subjects[0]
        data.columns = [self.datetime_string] + subjects

        for column in data.iloc[:, 1:]:
            data[column] = data[column].str.replace(",", ".").astype(float)
        return data
