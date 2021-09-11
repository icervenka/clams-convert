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
