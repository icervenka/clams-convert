class FileFormatError(Exception):
    """Raise if the input file doesn't have required specification"""
    pass


class SubjectIdError(Exception):
    """Raise if the input file is not an animal CLAMS file"""
    pass

class HeaderNotUnique(Exception):
    """Raise if the column names in the data header are not unique. Required for
        proper dataframe-wide calculations."""
    pass

class AggregationFrequencyError(Exception):
    """Raise if the specified aggregation frequency is not allowed, in the case
        that it would lead to disrupting day-night phase changes."""
    pass
