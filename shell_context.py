from clams_convert.datafile import Datafile
from clams_convert.col_mapper import ColMapper
from clams_convert.custom_parser import ClamsTseParser


cm = ColMapper('clams-tse')
tse = ClamsTseParser("%Y-%m-%d %H:%M:%S", mapper=cm)
fl = tse.parse("test/test_input/tse_1/20190923_TSE_raw_data.csv")
tsedf = Datafile(fl)

