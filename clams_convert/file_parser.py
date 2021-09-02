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
