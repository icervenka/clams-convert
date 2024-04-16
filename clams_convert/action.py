import numpy as np
import pandas as pd
import operator
import string
import random
import logging
from datetime import datetime
from abc import abstractmethod
from . import errors as e
from .file_scanner import FileScanner
from .custom_parser import AnalysisVisParser
from .col_mapper import ColMapper
from .datafile import Datafile


class Action:

    accepted_extensions = ["csv", "txt", "tsv", "asc"]
    metadata_anchor = "[Metadata]"
    data_anchor = "[Data]"

    def __init__(self, cmd, datafiles=None):
        self.cmd = cmd
        self.parser = None
        # self.scanner = file_scanner.factory_file_scanner(self.cmd.get('input'), Action.accepted_extensions)
        self.scanner = FileScanner(self.cmd.get('input'), Action.accepted_extensions)
        self.files = self.scanner.scan_files()
        self.common_interval_freq = None
        logging.basicConfig(filename=self.cmd.get('output') + '/clams-convert.log',
                            level=logging.DEBUG,
                            format='%(message)s')
        self.logger = logging.getLogger('clams-convert')
        if datafiles is None:
            self.datafiles = []
        else:
            [self.add_datafile(x) for x in datafiles]

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def run(self, *args):
        pass

    def validate_aggregation(self):
        if self.cmd.get('frequency') != 0 and self.cmd.get('frequency') is not None:
            datafile_frequences = [x.freq for x in self.datafiles]
            if np.sum(np.remainder(self.cmd.get('frequency'), datafile_frequences)) != 0:
                raise ValueError("Datafiles cannot be aggregated to specified frequency")

    def add_datafile(self, source):
        if isinstance(source, Datafile):
            self.datafiles.append(source)
        else:
            self.datafiles.append(Datafile(source))
        self.find_common_interval()
        return self

    def find_common_interval(self):
        all_intervals_s = [x.freq for x in self.datafiles]
        self.common_interval_freq = np.lcm.reduce(all_intervals_s)
        self.logger.info("Common interval frequency identified in datafiles: {} s".format(self.common_interval_freq))
        if self.common_interval_freq // 60 < 1:
            info_string = "The common interval is lower than one minute, is it intentional?"
            self.logger.warning(info_string)
            # raise UserWarning(info_string)
        return self

    def order_datafiles(self, by):
        self.datafiles = sorted(self.datafiles, key=operator.attrgetter(by))
        return self

    def regularize(self):
        data = [x.regularize() for x in self.datafiles]
        return data

    def create_metadata(self, meta_dict):
        metadata_info = dict(
            filetype="analysis-vis",
            orientation=self.cmd.get('orientation'),
            multiparameter=str(self.parser.format_description['multiparameter']),
        )
        metadata_info.update(**meta_dict)
        return pd.DataFrame(metadata_info.items())

    def export(self, datafiles):
        for x in datafiles:
            filename = datetime.today().strftime('%Y%m%d') + "_" + \
                       str(random.randint(100, 999)) + "_" + \
                       type(self).__name__.lower() + ".csv"
            with open(str(self.cmd.get('output')) + "/" + filename, "w", newline='') as file:
                file.write(Action.metadata_anchor + "\n")
                self.create_metadata(dict()).to_csv(file, mode="a", index=False, header=False)
                file.write(Action.data_anchor + "\n")
                x.reorient(self.cmd.get('orientation')).export(file)

    @staticmethod
    def join_datafiles(datafiles):
        merged = pd.concat([x.data for x in datafiles], axis=0)
        return Datafile(merged)

    @staticmethod
    def join_rows(df_list):
        return pd.concat(df_list, axis=0)

    def __str__(self):
        print(vars(self))

# class Convert:

#     def __init__(self, parser, cmd):
#         self.cmd = cmd
#         self.scanner = FileScanner(self.cmd.input, ["csv", "CSV"])
#         self.mapper = ColMapper(self.cmd.system)
#         self.parser = parser(self.cmd.time_fmt_in, self.mapper)

#     def process(self):
#         files = self.scanner.scan_files()
#         for file in files:
#             try:
#                 p = self.parser(self.cmd.time_fmt_in, self.mapper)
#                 data = p.parse(file)
#             except (FileFormatError, SubjectIdError, ValueError) as err:
#                 pass
#         return data

#     def export(self):
#         print("Processing complete")
#         filename = str(datetime.date.today()) + "_" + self.cmd.system + ".csv"
#         with open(str(self.cmd.output) + "/" + filename, "w", newline='') as file:
#             self.process().to_csv(file, index=False)
#         print("\n")
#         print("Results were saved to file: " + filename)
#         #TODO add analysis-vis specific header

#     #TODO
#     def log(self):
#         pass

#     def __str__(self):
#         print(vars(self))

class Convert(Action):

    def __init__(self, parser, *args):
        super().__init__(*args)
        self.mapper = ColMapper(self.cmd.get('system'))
        self.parser = parser(self.cmd.get('time_fmt_in'), self.mapper)

    def validate(self):
        self.validate_aggregation()
        return self

    # def run_one(self, *args):
    #     try:
    #         self.logger.info("Processing: " + self.files[0])
    #         data = self.parser.parse(self.files[0])
    #     except (e.FileFormatError, e.SubjectIdError, ValueError) as err:
    #         raise err
    #     return [Datafile(data)]
    #
    # def run_many(self, *args):
    #     combined_data = []
    #     for file in self.files:
    #         try:
    #             self.logger.info("Processing: " + file)
    #             data = self.parser.parse(file)
    #             combined_data.append(data)
    #         except (e.FileFormatError, e.SubjectIdError, ValueError) as err:
    #             raise err
    #     if self.parser.format_description['multifile'] is True:
    #         return [Datafile(pd.concat(combined_data, axis=0))]
    #     else:
    #         return [Datafile(x) for x in combined_data]

    def run(self, *args):
        print("\nConverting files...\n")
        if isinstance(self.scanner, file_scanner.IndividualFileScanner):
            datafile_list = self.run_one(*args)
        else:
            datafile_list = self.run_many(*args)
        self.validate()
        if self.cmd.get('regularize'):
            datafile_list = [x.regularize() for x in datafile_list]
        if self.cmd.get('frequency') != 0:
            datafile_list = [x.aggregate(self.cmd.get('frequency'), how=dict(np.sum)) for x in datafile_list]
        return datafile_list


class Join(Action):

    def __init__(self, *args):
        super().__init__(*args)
        self.parser = AnalysisVisParser()

    def validate(self):
        self.validate_aggregation()
        # current version support only joining file with the same frequency
        if len(set([x.freq for x in self.datafiles])) != 1:
            raise ValueError("Joining experiments with different frequency is not currently supported")
        for f, s in zip(self.datafiles, self.datafiles[1:]):
            if f.end_date > s.start_date:
                raise ValueError("Times of datafiles to be arranged are overlapping")
        return self

    def run(self, *args):
        print("\nJoining files...\n")
        for file in self.files:
            try:
                self.logger.info("Processing: " + file)
                self.add_datafile(file)
            except (e.FileFormatError, e.SubjectIdError, ValueError) as err:
                raise err
        self.order_datafiles("start_date").validate()
        regularized_datafiles = self.regularize()
        merged_data = Datafile(pd.concat([x.data for x in regularized_datafiles], axis=0)).regularize()
        if self.cmd.get('frequency') != 0:
            merged_data = merged_data.aggregate(self.cmd.get('frequency'), how=dict(np.sum))
        # merged_data = Datafile(pd.concat(self.regularize(), axis=0)).regularize()
        return [merged_data]

class Match(Action):

    def __init__(self, *args):
        super().__init__(*args)
        self.parser = AnalysisVisParser()
        self.dark_start = self.cmd.get('dark_start')
        self.dark_end = self.cmd.get('dark_end')

    @staticmethod
    def timedelta_from_str(str1, str2):
        return datetime(str2) - datetime(str1)

    def validate(self):
        self.validate_aggregation()
        # dark_durations = [self.timedelta_from_str(d.dark_end - d.dark_start) for d in self.datafiles]
        # if (not set(dark_durations)) | len(set(dark_durations)) > 1:
        #     raise ValueError("Durations of dark cycles for individual experiments differ.")
        return self

    def run(self, *args):
        print("\nMatching files...\n")
        letters = iter(string.ascii_letters)
        for file in self.files:
            try:
                # TODO add original filename as suffix to subject name
                self.logger.info("Processing: " + file)
                add_letter = next(letters)
                d = Datafile(file)
                name_mapping = dict(zip(d.subjects, [x + add_letter for x in d.subjects]))
                d = d.rename_subjects(name_mapping)
                self.logger.info(str(file))
                self.add_datafile(d)
            except (e.FileFormatError, e.SubjectIdError, ValueError):
                raise
        self.validate()

        if self.cmd.get('frequency') != 0:
            agg_freq = self.cmd.get('frequency')
        else:
            agg_freq = self.common_interval_freq

        matched_datafiles = []
        for d in self.datafiles:
            # takes dict as aggregation mapping
            # TODO some datafiles can have start some end
            d = d.regularize().remove_incomplete_cycle(self.cmd.get('dark_start'), self.cmd.get('dark_end'))
            # TODO change to universal mapping
            d = d.aggregate(str(agg_freq) + 's', dict(distance=np.sum))
            matched_datafiles.append(d)

        # TODO can also be dark_end
        datafiles = [x.set_datetime_start(self.cmd.get('match_start'), self.cmd.get('dark_start')) for x in matched_datafiles]
        merged_data = self.join_datafiles(datafiles).equalize_observations()
        return [merged_data]
