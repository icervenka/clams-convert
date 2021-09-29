import numpy as np
import pandas as pd
import collections
import traces
import os
import logging
from datetime import timedelta, datetime, time, date
from .custom_parser import AnalysisVisParser
from . import errors as e

def round_minutes(dt, how="up"):
    if how == "up":
        return dt + timedelta(seconds=(60-dt.second))
    elif how == "down":
        return dt + timedelta(seconds=dt.second)
    else:
        return None

def str_to_time(time_string, sep=":"):
    if len(sep) != 1:
        raise ValueError("Time separator is not of length 1")
    t = time(*map(int, time_string.split(sep)))
    return t

def freq_to_seconds(freq):
    return int(freq.total_seconds())

def create_parameter_ts(df, parameter):
    # TODO casting all to float might not work
    ts = traces.TimeSeries(dict(zip(df.date_time, df[parameter].astype(float))))
    return ts

def traces_to_pandas(ts_dict):
    per_subject = []
    for s, subjects in ts_dict.items():
        per_param = []
        for p, parameter in subjects.items():
            param_df = pd.DataFrame(parameter).set_index(0)
            param_df.index.name = "date_time"
            param_df.columns = [p]
            per_param.append(param_df)
        subject_df = pd.concat(per_param, axis=1).reset_index()
        subject_df.insert(1, "interval", list(range(len(subject_df.index))))
        subject_df.insert(0, "subject", s)
        per_subject.append(subject_df)
    return pd.concat(per_subject, axis=0)

def create_datetime_series(self, start_date, start_time, periods, freq):
    start = datetime.strptime(start_date + " " + start_time, "%Y-%m-%d %H:%M:%S")
    dt = pd.date_range(start=start, periods=periods, freq=str(freq)+'s')
    return dt


def find_common_divisors(a, b):
    a = int(a)
    b = int(b)
    divisors = []
    for i in range(1, min(a, b)+1):
        if a%i == 0 and b%i == 0:
            divisors.append(i)
    return divisors

def divisible(arr, a):
    a = int(a)
    return [int(x) for x in arr if int(x)%a == 0]


class Datafile:

    def __init__(self, datafile, dark_start = None, dark_end = None, force_regularize=True):
        self.parser = AnalysisVisParser("%Y-%m-%d %H:%M:%S")
        self.id = None
        self.start_date = None
        self.end_date = None
        self.dark_start = dark_start
        self.dark_end = dark_end
        self.phase_change_indices = tuple()
        self.phase_change_dates = tuple()
        self.subject_freq = dict()
        self.freq = None
        self.allowed_agg_freq = None
        self.regular = True
        self.force_regularize = force_regularize
        # None, nudge or interpolate
        # TODO implement nudge
        self.regularization_method = regularization_method
        self.initialized = False
        self.num_observations = dict()
        self.subjects = []
        self.descriptors = ["subject", "date_time", "interval"]
        self.light_column = ["light"]
        self.parameters = []
        self.data = None
        self.subject_split_data = collections.OrderedDict()
        self.logger = logging.getLogger('clams-convert')
        self.__create(datafile)

    def __create(self, source):
        if isinstance(source, str):
            if os.path.exists(source):
                self.data = self.parser.parse(source)
        elif isinstance(source, pd.DataFrame):
            self.data = source
            self.data.date_time = pd.to_datetime(self.data.date_time,
                                                 format="%Y-%m-%d %H:%M:%S")
        else:
            raise ValueError("Datafile source is of unrecognized type, " +
                "only path and DataFrame are accepted.")
        self.__initialize()
        return self

    def init_start_end_date(self, round_mins=True):
        self.start_date = sorted(self.data.date_time)[0]
        self.end_date = sorted(self.data.date_time)[-1]
        if round_mins:
            self.start_date = round_minutes(self.start_date)
            self.end_date = round_minutes(self.end_date)

    def init_subjects(self, rename_subject_mapping):
        if rename_subject_mapping is not None:
            self.rename_subjects(rename_subject_mapping)
        self.subjects = list(set(self.data.subject.tolist()))
        for s in self.subjects:
            self.subject_split_data[s] = self.data.query('subject == @s')

    def init_num_observations(self):
        try:
            self.num_observations = {k: s.shape[0] for (k, s) in self.subject_split_data.items()}
        except ValueError:
            print("Subject data not initialized.")

    def init_parameters(self):
        self.parameters = self.get_parameters()

    def init_light_column(self):
        if self.dark_start is None or self.dark_end is None:
            raise ValueError("Light column is not present in the data " +
                "and dark_start or dark_end attributes are not set.")
        else:
            ts = self.data['date_time'].time()
            ds = str_to_time(self.dark_start)
            de = str_to_time(self.dark_end)
            # TODO this will not work
            self.data['light'] = pd.Series(np.where(ts >= ds and ts < de, 0, 1))

    def init_phase_changes(self):
        if self.data['light'].iloc[0] == 0:
            next_phase = 1
        elif self.data['light'].iloc[0] == 1:
            next_phase = 0
        else:
            raise ValueError("Illegal value in the light column, only 0 and 1 are permitted.")
        i1 = list(self.data['light']).index(next_phase)
        i2 = list(self.data['light'].iloc[(i1+1):]).index(1-next_phase)
        self.phase_change_indices = (i1, i2)
        self.phase_change_dates = (self.data.date_time.iloc[i1],
                                   self.data.date_time.iloc[i2])

    def init_freq(self):
        try:
            interval_lengths = self.subject_interval_lengths()
            subject_freq = self.find_freq(interval_lengths)
            self.validate_freq(subject_freq)
            self.freq = freq_to_seconds(list(set(subject_freq.values()))[0])
        except ValueError:
            print("Subject data not initialized.")


    def init_allowed_agg_freq(self):
        phase1_duration = self.phase_change_dates[1] - self.phase_change_dates[0]
        phase2_duration = timedelta(hours=24) - phase1_duration
        common_agg = find_common_divisors(phase1_duration.total_seconds(),
                                          phase2_duration.total_seconds())
        self.allowed_agg_freq = divisible(common_agg, self.freq)

    def get_parameters(self):
        t = list(self.data.columns)
        _ = [t.remove(x) for x in self.descriptors]
        _ = [t.remove(x) for x in self.light_column]
        return t

    # Block of initialize and initialize-related functions
    #

    # TODO currently does not interfaced with class
    # Intended mainly for commandline, find a way how to include in html-version
    def rename_subjects(self, name_mapping):
        if callable(name_mapping):
            self.data_subjects = name_mapping(self.data_subjects)
        elif isinstance(name_mapping, dict):
            #TODO affects split_subject_data
            for key, value in name_mapping.items():
                new_subjects = self.data.subjects.str.replace('^' + key + "$", value)
            # if inplace:
            self.data.subjects = new_subjects
        else:
            raise TypeError("Name mapping has to be either function or dict.")
        return self
        # else:
        #     return Datafile(self.data).rename_subjects(a, inplace=True)

    def subject_interval_lengths(self):
        interval_lengths = dict()
        for subject, data in self.subject_split_data.items():
            date_column = data.date_time #pd.to_datetime(data.date_time)
            count_intervals = collections.Counter(date_column.diff())
            count_intervals.pop(pd.NaT, None)
            interval_lengths[subject] = count_intervals
        return interval_lengths

    def find_freq(self, interval_lengths):
        subject_freq = dict()
        for k, v in interval_lengths.items():
            if len(v) > 1 and not self.force_regularize:
                raise ValueError("The time series for {} is not regular ".format(k) +
                                 "and force regularization is turned off")
            elif len(v) > 1 and self.force_regularize:
                self.logger.info("Time series for subject {} is not regular. \n".format(k) +
                                 "The following are present (interval:count)")
                for value, count in v.most_common():
                    self.logger.info("\t" + str(value) + ":" + str(count))
                self.logger.info("Selected most common interval - {}\n".format(v.most_common(1)[0][0]))
                subject_freq[k] = v.most_common(1)[0][0]
                self.regular = False
            else:
                subject_freq[k] = v.most_common(1)[0][0]
                self.regular = True
        return subject_freq

    def validate_freq(self, subject_freq):
        try:
            subject_freq_list = list(set(subject_freq.values()))
            if len(subject_freq_list) > 1:
                raise ValueError(
                    "Subjects in the experiment have different time intervals " +
                    "between measurements. Consider converting the subject " +
                    "files individually and use 'match' utility to combine " +
                    "them with common measurement frequency.")
        except ValueError:
            ("No measurement intervals detected in data file.")

    # Block of regularize and regularize-related functions
    #

    def equalize_observations(self, remove_from_end = True):
        o = min(self.num_observations.values())
        if remove_from_end:
            filtered_data = {k: s[0:o] for (k, s) in self.subject_split_data.items()}
        else:
            filtered_data = {k: s[(len(s)-o):len(s)] for (k, s) in self.subject_split_data.items()}
        return Datafile(pd.concat(filtered_data.values(), axis=0))

    def remove_incomplete_cycle(self, remove_from_end = True):
        if self.first_phase_change == self.start_data:
            return self
        else:
            filtered_data = {}
            for k, s in self.subject_split_data.items():
                tmp = s[s.date_time >= self.first_phase_change]
                filtered_data[k] = tmp
            return Datafile(pd.concat(filtered_data, axis=0)).equalize_observations(remove_from_end)

    def regularize(self, inplace=False):
        if not self.freq:
            raise ValueError("Measurement frequency of dataset has not been set." +
                " Please run 'validate' first.")
        if not self.regular:
            self.logger.info("Regularizing on frequency {}".format(self.freq))
            results = dict()
            for i, s in self.subject_split_data.items():
                results[i] = dict()
                for p in self.parameters:
                    ts = create_parameter_ts(s, p)
                    ts_reg = ts.sample(
                        sampling_period=timedelta(seconds=self.freq),
                        start=self.start_date,
                        end=self.end_date,
                        interpolate='linear',
                    )
                    results[i][p] = ts_reg
            if inplace:
                self.data = traces_to_pandas(results).__initialize()
                return self
            else:
                return Datafile(traces_to_pandas(results))
        else:
            return self

    # TODO
    # use different functions for different parameters - add aggregator to col_mapper
    # light aggregated min, max or maybe median - should not matter if phases are preserved
    # does not center on phase change
    def aggregate(self, new_freq, how):
        if new_freq not in self.allowed_agg_freq:
            raise e.AggregationFrequencyError("Illegal frequency, only frequencies" +
                                            "divisible that don't disrupt phase" +
                                            "changes are allowed.")
        aggregated_data = {}
        for k, s in self.subject_split_data.items():
            self.logger.info("Aggregating on frequency {}".format(new_freq))
            df = s.resample(new_freq, label='left', closed='left', origin='start',
                            on='date_time').agg(how).reset_index()
            df = df.drop('interval', 1) # remove interval so it can be inserted again
            df = df.rename_axis('interval').reset_index()
            df.insert(0, "subject", k)
            aggregated_data[k] = df
        return Datafile(pd.concat(aggregated_data.values(), axis=0))

    def reorient_data(self, orientation):
        if orientation == 'subject-wide':
            self.logger.info("Reorienting to subject-wide format")
            # might not meet the Datafile long format specifications
            df = self.data.melt(id_vars=["subject", "date_time", "interval", "light"],
                                var_name="parameter")
            df = df.pivot_table(index=["parameter", "date_time", "interval", "light"],
                                columns="subject").reset_index()
            return df
        else:
            return self.data

    def set_datetime_start(self, start_date, start_time):
        modified_data = {}
        for k, s in self.subject_split_data.items():
            new_dt = create_datetime_series(start_date, start_time, s.shape[0], self.freq)
            with pd.option_context('mode.chained_assignment', None):
                s.date_time = new_dt
            modified_data[k] = s
        return Datafile(pd.concat(modified_data.values(), axis=0))

    def export(self, file):
        self.logger.info("Exporting to file: {}".format(file))
        self.data.to_csv(file, mode="a", index=False, header=True)
