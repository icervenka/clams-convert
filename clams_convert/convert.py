import datetime
from .col_mapper import ColMapper
from .file_scanner import FileScanner
from .file_parser import SubjectIdError
from .file_parser import FileFormatError


class Convert:

    def __init__(self, parser, cmd):
        self.cmd = cmd
        self.scanner = FileScanner(self.cmd.input, ["csv", "CSV"])
        self.mapper = ColMapper(self.cmd.system)
        self.parser = parser(self.cmd.time_fmt_in, self.mapper)

    def process(self):
        files = self.scanner.scan_files()
        for file in files:
            try:
                p = self.parser(self.cmd.time_fmt_in, self.mapper)
                data = p.parse(file)
            except (FileFormatError, SubjectIdError, ValueError) as err:
                pass
        return data

    def export(self):
        print("Processing complete")
        filename = str(datetime.date.today()) + "_" + self.cmd.system + ".csv"
        with open(str(self.cmd.output) + "/" + filename, "w", newline='') as file:
            self.process().to_csv(file, index=False)
        print("\n")
        print("Results were saved to file: " + filename)
        #TODO add analysis-vis specific header

    #TODO
    def log(self):
        pass

    def __str__(self):
        print(vars(self))
