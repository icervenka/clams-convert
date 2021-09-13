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
