import classes.datafile as ex
import classes.action as action
import classes.custom_parser as fp
import importlib
import json
import os

available_parsers = {"clams-oxymax": fp.ClamsOxymaxParser,
                     "clams-tse": fp.ClamsTseParser,
                     "fwr-zierathold": fp.FwrZierathOldParser,
                     "fwr-zierath": fp.FwrZierathParser,
                     "fwr-canlon": fp.FwrCanlonParser,
                     "fwr-westerblad": fp.FwrWesterbladParser}

input_path_prefix = "test/test_input/"
output_path_prefix = "test/test_output/"

tests = json.load("tests/test.json")

for item in tests:
    print(item)
    item['args']['input'] = input_path_prefix + item['args']['input']
    item['args']['output'] = output_path_prefix + item['args']['output']
    if item['action'] is "Convert":
        # init_class = globals()[item['action']]
        a = action.Convert(available_parsers[item['parser']], item['args'])
    elif item['action'] is "Join":
        a = action.Join(item['args'])
    elif item['action'] is "Match":
        a = action.Match(item['args'])
    b = a.run()
    c = a.export(b)

#def main():
# cmd = {}
# cmd['input'] = "/Users/igocer/OneDrive/programming/clams-convert/test_suites/zierathold_1"
# cmd['output'] = "/Users/igocer/OneDrive/programming/clams-convert/test_suites/zierathold_1"
# # cmd['input'] = "E:\OneDrive\programming\clams-convert\\test_paula_2"
# # cmd['output'] = "E:\OneDrive\programming\clams-convert\\test_paula_2_convert"
# cmd['dark_start'] = "18:00:00"
# cmd['dark_end'] = "06:00:00"
# cmd['match_start'] = "1970-01-01"
# cmd['system'] = "fwr-zierath"
# cmd['time_fmt_in'] = "%d/%m/%Y %H:%M:%S"
# print(cmd)
# a = action.Convert(fp.FwrZierathParser, cmd)
# b = a.run()
# a.export(b)


# cmd = dict()
# cmd['input'] = "E:\OneDrive\programming\clams-convert\\test_paula_1_convert"
# cmd['dark_start'] = "18:00:00"
# cmd['dark_end'] = "06:00:00"
# cmd['match_start'] = "1970-01-01"
# cmd['system'] = "fwr-zierathold"
# cmd['output'] = "E:\OneDrive\programming\clams-convert\\test_paula_1_join"
# print(cmd)
# a = action.Join(cmd)
# b = a.run()
# a.export(b)

# cmd = dict()
# cmd['input'] = "E:\OneDrive\programming\clams-convert\\test_paula_match"
# cmd['output'] = "E:\OneDrive\programming\clams-convert\\test_paula_match_out"
# # cmd['input'] = "/Users/igocer/OneDrive/programming/clams-convert/test_paula_match"
# # cmd['output'] = "/Users/igocer/OneDrive/programming/clams-convert/test_paula_match_out"
# cmd['dark_start'] = "18:00:00"
# cmd['dark_end'] = "06:00:00"
# cmd['match_start'] = "1970-01-01"
# cmd['frequency'] = 0
# cmd['orientation'] = "parameter-wide"
#
# print(cmd)
# a = action.Match(cmd)
# b = a.run()
# a.export(b)

# import pandas as pd
# df = pd.read_csv("20200506_102_match.csv", sep = ",", skiprows=5)
# df.distance = df.distance.round(3)
#
# df2 = df.pivot_table(index=["date_time", "interval"], columns="subject", values="distance").reset_index()
#
# df2.to_csv("running_combined.txt", sep="\t", index = False)



# test_dir = "OneDrive/programming/clams-convert/test_suite/"
#
# cmd_common = dict(
#     dark_start="18:00:00",
#     dark_end="06:00:00",
#     match_start="1970-01-01",
#     frequency=0,
#     orientation="parameter-wide",
#     output=test_dir+"test_output"
# )
#
# tests = [
#     dict(
#         action="convert",
#         system="fwr-zierathold",
#         input=test_dir+"zierathold_1"
#     ),
#     dict(
#         action="convert",
#         system="fwr-zierath",
#         input=test_dir+"zierath_1"
#     ),
#     dict(
#         action="convert",
#         system="fwr-zierath",
#         input=test_dir + "zierath_2"
#     ),
#     dict(
#         action="convert",
#         system="fwr-zierath",
#         input=test_dir + "zierath_3"
#     ),
#     dict(
#         action="convert",
#         system="clams-oxymax",
#         input=test_dir+"classic_1"
#     ),
#     dict(
#         action="convert",
#         system="clams-oxymax",
#         input=test_dir + "classic_2"
#     ),
#     dict(
#         action="convert",
#         system="clams-oxymax",
#         input=test_dir + "classic_3"
#     ),
#     dict(
#         action="convert",
#         system="clams-tse",
#         input=test_dir+"tse_1"
#     ),
#     dict(
#         action="convert",
#         system="clams-tse",
#         input=test_dir + "tse_2"
#     ),
#     dict(
#         action="convert",
#         system="clams-tse",
#         input=test_dir + "tse_3"
#     )
# ]
#
# for item in tests:
#     item.update(**cmd_common)
#     item.get('action')(item)
