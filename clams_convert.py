#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data conversion tool from various activity and metabolic monitoring systems
to formats suitable for analysis by activity-vis web based tool.
"""

import argparse
import clams_convert.custom_parser as fp
from clams_convert.action import Convert
from clams_convert.action import Join
from clams_convert.action import Match

available_parsers = {"clams-oxymax": fp.ClamsOxymaxParser,
                     "clams-tse": fp.ClamsTseParser,
                     "fwr-zierathold": fp.FwrZierathOldParser,
                     "fwr-zierath": fp.FwrZierathParser,
                     "fwr-canlon": fp.FwrCanlonParser,
                     "fwr-westerblad": fp.FwrWesterbladParser}

# TODO only detect parser in convert
# def validate_args(args):
#     if args.system not in available_parsers.keys():
#         raise ValueError("Unsupported system type. Please select one of {}".format(available_parsers))


def convert(args):
    return Convert(available_parsers[args.system], args)


def join(args):
    return Join(args)


def match(args):
    return Match(args)


def main():
    # parse input and output arguments arguments
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action",
                                       title="valid actions")

    # ------------------------------------------------------------------------------------------------------------------
    # parent parser
    # ------------------------------------------------------------------------------------------------------------------
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('-i', '--input',
                               type=str,
                               help="Path to input files, default: current dir")
    parent_parser.add_argument('-o', '--output',
                               type=str,
                               help="Path where the output will be stored, default: current dir")
    parent_parser.add_argument('--log',
                               type=str,
                               help="Path where the log information will be stored, default: stdout")
    parent_parser.add_argument('--pattern',
                               help="String pattern to match input files")
    parent_parser.add_argument('--frequency',
                               type=int,
                               help="Aggregate the output data to selected frequency in seconds." +
                                    "Has to be a valid multiple of input frequency. Default 0 - no custom aggregation",
                               default=0)
    parent_parser.add_argument('--out_file_suffix',
                               type=str,
                               help="Suffix of the exported csv file. The 'action' name is used by default.")
    parent_parser.add_argument('--orientation',
                               type=str,
                               choices=("parameter-wide", "subject-wide"),
                               default="parameter-wide",
                               help="Orientation of output csv file. Can be either with subjects as columns or with " +
                                    "parameters as columns. Only parameter-wide format is accepted by analysis-vis tool.")
    parent_parser.add_argument('--time_fmt_in',
                               type=str,
                               help="Date-Time format used in source files. Accepts strftime format strings",
                               default="%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------------------------------------------------------------------
    # convert argparse
    # ------------------------------------------------------------------------------------------------------------------
    parser_convert = subparsers.add_parser('convert',
                                           parents=[parent_parser],
                                           help="Converts files to a common specification accepted by activity-vis" +
                                                " programs as well as 'match' and join' sub-commands.")
    parser_convert.add_argument('--system', type=str, help="System which was used to record the data",
                                choices=available_parsers.keys(),
                                default="clams-oxymax")
    parser_convert.add_argument('--col_spec',
                                type=str,
                                help="Specification file for column specs if custom system is used.")
    parser_convert.add_argument('--regularize', action='store_true', help="Make time series regular")
    parser_convert.set_defaults(action=convert)

    # ------------------------------------------------------------------------------------------------------------------
    # join argparse
    # ------------------------------------------------------------------------------------------------------------------
    parser_join = subparsers.add_parser('join',
                                        parents=[parent_parser],
                                        help="Joins together experiment files one after the other, filling in " +
                                             "the missing times if appropriate.")
    parser_join.set_defaults(action=join)

    # ------------------------------------------------------------------------------------------------------------------
    # match argparse
    # ------------------------------------------------------------------------------------------------------------------
    parser_match = subparsers.add_parser('match',
                                         parents=[parent_parser],
                                         help="Unify several experiment files to the same time range and time scale.")
    parent_parser.add_argument('--dark_start',
                               type=str,
                               help="Start of dark cycle if light column in not present in the data.",
                               default="18:00:00")
    parent_parser.add_argument('--dark_end',
                               type=str,
                               help="End of dark cycle if light column in not present in the data.",
                               default="06:00:00")
    parser_match.add_argument('--match_start',
                              type=str,
                              default="1970-01-01",
                              help="After matching several experiments, one " +
                                   "arbitrary date can to be chosen as common. Please use " +
                                   "format 'yyyy-mm-dd'. If none is specified, 1970-01-01 " +
                                   "will be used as default.")
    parser_match.set_defaults(action=match)

    # ------------------------------------------------------------------------------------------------------------------
    # run
    # ------------------------------------------------------------------------------------------------------------------
    args = parser.parse_args()
    #validate_args(args)

    act = args.action(args)
    result = act.run()
    act.export(result)

if __name__ == "__main__":
    main()
