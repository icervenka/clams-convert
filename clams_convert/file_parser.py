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
