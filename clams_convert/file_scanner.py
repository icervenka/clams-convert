import glob
import os

class FileScanner:

    def __init__(self, path, extensions=None, pattern=None, exclude_pattern=None):
        self.path = path
        self.pattern = ("" if pattern is None else pattern)
        self.exclude_pattern = exclude_pattern
        self.accepted_extensions = extensions

    def scan_files(self):
        files = []
        if os.path.isfile(self.path):
            files = [self.path]
        elif os.path.isdir(self.path):
            for ext in self.accepted_extensions:
                for file in glob.glob(self.path + "/*" + self.pattern + "*." + ext):
                    files.append(file)
            if self.exclude_pattern is not None:
                files = [x for x in files if self.exclude_pattern not in x]
        else:
            raise ValueError("Specified location is neither file nor directory.")

        self.validate(files)
        return files

    def validate(self, files):
        if len(files) == 0:
            raise ValueError("No files to process.\n" +
                             "Accepted extensions are: {}\n".format(",".join(self.accepted_extensions)) +
                             "Exclude pattern: {}\n".format(self.exclude_pattern) +
                             "Exiting...")
        return None

    def __str__(self):
        print(vars(self))