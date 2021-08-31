import glob
import os

# def factory_file_scanner(path, *args):
#     if os.path.isdir(path):
#         return FileScanner(path, *args)
#     elif os.path.isfile(path):
#         return IndividualFileScanner(path, *args)
#     else:
#         raise ValueError("Specified location is neither file nor directory.")


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

# class DirFileScanner(FileScanner):
#
#     def __init__(self, *args):
#         super.__init__()
#
#     def scan_files(self):
#         pass
#
#
# class IndividualFileScanner(FileScanner):
#
#     def __init__(self, *args):
#         super.__init__(*args)
#
#     def scan_files(self):
#         files = []
#         for file in glob.glob(self.path):
#             files.append(file)
#         self.validate(files)
#         return files
#
#     def validate(self, files):
#         if len(files) != 1:
#             raise ValueError("Too many files matching the specified input.\n" +
#                              "Exiting...")
