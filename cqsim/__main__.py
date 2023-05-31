import json
import optparse
import os
import re
import sys
import time
from datetime import datetime

from cqsim import cqsim_main, cqsim_path


def datetime_strptime(value: str, format: str):
    """Parse a datetime like datetime.strptime in Python >= 2.5"""
    return datetime(*time.strptime(value, format)[0:6])


class Option(optparse.Option):

    """An extended optparse option with cbank-specific types.

    Types:
    date -- parse a datetime from a variety of string formats
    """

    DATE_FORMATS = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%y-%m-%d",
        "%y-%m-%d %H:%M:%S",
        "%y-%m-%d %H:%M",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%Y%m%d",
    ]

    def check_date(self, opt, value):
        """Parse a datetime from a variety of string formats."""
        for format in self.DATE_FORMATS:
            try:
                dt = datetime_strptime(value, format)
            except ValueError:
                continue
            else:
                # Python can't translate dates before 1900 to a string,
                # causing crashes when trying to build sql with them.
                if dt < datetime(1900, 1, 1):
                    raise optparse.OptionValueError(
                        "option %s: date must be after 1900: %s" % (opt, value)
                    )
                else:
                    return dt
        raise optparse.OptionValueError("option %s: invalid date: %s" % (opt, value))

    TYPES = optparse.Option.TYPES + ("date",)

    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER["date"] = check_date


def callback_alg(option, opt_str, value: str, parser):
    default_opt["alg"].append(value)
    return


def callback_alg_sign(option, opt_str, value: str, parser):
    default_opt["alg_sign"].append(value)
    return


def callback_bf_para(option, opt_str, value: str, parser):
    default_opt["bf_para"].append(value)
    return


def callback_win_para(option, opt_str, value: str, parser):
    default_opt["win_para"].append(value)
    return


def callback_ad_win_para(option, opt_str, value: str, parser):
    default_opt["ad_win_para"].append(value)
    return


def callback_ad_bf_para(option, opt_str, value: str, parser):
    default_opt["ad_bf_para"].append(value)
    return


def callback_ad_alg_para(option, opt_str, value: str, parser):
    default_opt["ad_alg_para"].append(value)
    return


def path_without_extension(file_name: str):
    return os.path.splitext(file_name)[0]


def alg_sign_check(alg_sign_t, leng):
    alg_sign_result = []
    temp_len = len(alg_sign_t)
    i = 0
    while i < leng:
        if i < temp_len:
            alg_sign_result.append(int(alg_sign_t[i]))
        else:
            # alg_sign_result.append(int(alg_sign_t[temp_len-1]))
            alg_sign_result.append(0)
        i += 1
    return alg_sign_result


def get_list(inputstring, regex):
    return re.findall(regex, inputstring)


if __name__ == "__main__":
    default_opt: cqsim_main.OptionalParaList = {
        "alg": [],
        "alg_sign": [],
        "bf_para": [],
        "win_para": [],
        "ad_win_para": [],
        "ad_bf_para": [],
        "ad_alg_para": [],
        "log_freq": 1,
        "read_input_freq": 1000,
    }

    p = optparse.OptionParser(option_class=Option)
    # 1
    p.add_option(
        "-j",
        "--job",
        dest="job_trace",
        type="string",
        help="file name of the job trace",
    )
    p.add_option(
        "-n",
        "--node",
        dest="node_struc",
        type="string",
        help="file name of the node structure",
    )
    p.add_option(
        "-J",
        "--job_save",
        dest="job_save",
        type="string",
        help="file name of the formatted job data",
    )
    p.add_option(
        "-N",
        "--node_save",
        dest="node_save",
        type="string",
        help="file name of the formatted node data",
    )
    p.add_option(
        "-f",
        "--frac",
        dest="cluster_fraction",
        type="float",  # default=1.0, \
        help="job density adjust",
    )

    # 6
    p.add_option(
        "-s",
        "--start",
        dest="start",
        type="float",  # default=0.0, \
        help="virtual job trace start time",
    )
    p.add_option(
        "-S",
        "--start_date",
        dest="start_date",
        type="date",
        help="job trace start date",
    )
    p.add_option(
        "-r",
        "--anchor",
        dest="anchor",
        type="int",  # default=0, \
        help="first read job position in job trace",
    )
    p.add_option(
        "-R",
        "--read",
        dest="read_num",
        type="int",  # default=-1, \
        help="number of jobs read from the job trace",
    )
    p.add_option(
        "-p",
        "--pre",
        dest="pre_name",
        type="string",  # default="CQSIM_", \
        help="previous file name",
    )

    # 11
    p.add_option(
        "-o",
        "--output",
        dest="output",
        type="string",
        help="simulator result file name",
    )
    p.add_option("--debug", dest="debug", type="string", help="debug file name")
    p.add_option(
        "--ext_fmt_j",
        dest="ext_fmt_j",
        type="string",  # default=".csv", \
        help="temp formatted job data extension type",
    )
    p.add_option(
        "--ext_fmt_n",
        dest="ext_fmt_n",
        type="string",  # default=".csv", \
        help="temp formatted node data extension type",
    )
    p.add_option(
        "--ext_fmt_j_c",
        dest="ext_fmt_j_c",
        type="string",  # default=".con", \
        help="temp job trace config extension type",
    )

    # 16
    p.add_option(
        "--ext_fmt_j_n",
        dest="ext_fmt_n_c",
        type="string",  # default=".con", \
        help="temp job trace config extension type",
    )
    p.add_option(
        "--path_in",
        dest="path_in",
        type="string",  # default="Input Files/", \
        help="input file path",
    )
    p.add_option(
        "--path_out",
        dest="path_out",
        type="string",  # default="Results/", \
        help="output result file path",
    )
    p.add_option(
        "--path_fmt",
        dest="path_fmt",
        type="string",  # default="Temp/", \
        help="temp file path",
    )
    p.add_option(
        "--path_debug",
        dest="path_debug",
        type="string",  # default="Debug/", \
        help="debug file path",
    )

    # 21
    p.add_option(
        "--ext_jr",
        dest="ext_jr",
        type="string",  # default=".rst", \
        help="job result log extension type",
    )
    p.add_option(
        "--ext_si",
        dest="ext_si",
        type="string",  # default=".ult", \
        help="system information log extension type",
    )
    p.add_option(
        "--ext_ai",
        dest="ext_ai",
        type="string",  # default=".adp", \
        help="adapt information log extension type",
    )
    p.add_option(
        "--ext_d",
        dest="ext_debug",
        type="string",  # default=".log", \
        help="debug log extension type",
    )
    p.add_option(
        "-v",
        "--debug_lvl",
        dest="debug_lvl",
        type="int",  # default=4, \
        help="debug mode",
    )

    # 26
    p.add_option(
        "-a",
        "--alg",
        dest="alg",
        type="string",
        action="callback",
        callback=callback_alg,
        help="basic algorithm list",
    )
    p.add_option(
        "-A",
        "--sign",
        dest="alg_sign",
        type="string",
        action="callback",
        callback=callback_alg_sign,
        help="sign of the algorithm element in the list",
    )
    p.add_option(
        "-b", "--bf", dest="backfill", type="int", help="backfill mode"  # default=0, \
    )
    p.add_option(
        "-B",
        "--bf_para",
        dest="bf_para",
        type="string",
        action="callback",
        callback=callback_bf_para,
        help="backfill parameter list",
    )
    p.add_option(
        "-w", "--win", dest="win", type="int", help="window mode"  # default=0, \
    )

    # 31
    p.add_option(
        "-W",
        "--win_para",
        dest="win_para",
        type="string",
        action="callback",
        callback=callback_win_para,
        help="window parameter list",
    )
    p.add_option(
        "-l",
        "--ad_bf",
        dest="ad_bf",
        type="int",  # default=0, \
        help="backfill adapt mode",
    )
    p.add_option(
        "-L",
        "--ad_bf_para",
        dest="ad_bf_para",
        type="string",
        action="callback",
        callback=callback_ad_bf_para,
        help="backfill adapt parameter list",
    )
    p.add_option(
        "-d",
        "--ad_win",
        dest="ad_win",
        type="int",  # default=0, \
        help="window adapt mode",
    )
    p.add_option(
        "-D",
        "--ad_win_para",
        dest="ad_win_para",
        type="string",
        action="callback",
        callback=callback_ad_win_para,
        help="window adapt parameter list",
    )

    # 36
    p.add_option(
        "-g",
        "--ad_alg",
        dest="ad_alg",
        type="int",  # default=0, \
        help="algorithm adapt mode",
    )
    p.add_option(
        "-G",
        "--ad_alg_para",
        dest="ad_alg_para",
        type="string",
        action="callback",
        callback=callback_ad_alg_para,
        help="algorithm adapt parameter list",
    )
    p.add_option(
        "-c",
        "--config_n",
        dest="config_n",
        type="string",
        default="config_name.json",
        help="name config file",
    )
    p.add_option(
        "-C",
        "--config_sys",
        dest="config_sys",
        type="string",
        default="config_sys.json",
        help="system config file",
    )
    p.add_option(
        "-m", "--monitor", dest="monitor", type="int", help="monitor interval time"
    )

    # 41
    p.add_option("-I", "--log_freq", dest="log_freq", type="int", help="log frequency")

    p.add_option(
        "-z",
        "--read_input_freq",
        dest="read_input_freq",
        type="int",
        help="read input frequency",
    )

    opts, args = p.parse_args()

    inputPara: cqsim_main.OptionalParaList = default_opt
    opts.alg = default_opt["alg"]
    opts.alg_sign = default_opt["alg_sign"]
    opts.bf_para = default_opt["bf_para"]
    opts.win_para = default_opt["win_para"]
    opts.ad_win_para = default_opt["ad_win_para"]
    opts.ad_bf_para = default_opt["ad_bf_para"]
    opts.ad_alg_para = default_opt["ad_alg_para"]

    inputPara["resource_job"] = 0
    inputPara["resource_node"] = 0
    # 0:Read original file   1:Read formatted file
    inputPara = {x: inputPara[x] for x in inputPara if inputPara[x]}  # type: ignore

    if opts.config_sys:
        with open(os.path.join(cqsim_path.path_config, opts.config_sys)) as f:
            inputPara_name = json.load(f)
            inputPara = inputPara_name | inputPara

    if config_n := inputPara.get("config_n", None):
        opts.config_n = config_n
    if opts.config_n:
        with open(os.path.join(cqsim_path.path_config, opts.config_n)) as f:
            inputPara_name = json.load(f)
            inputPara = inputPara_name | inputPara

    if (
        not opts.job_trace
        and not opts.job_save
        and not inputPara.get("job_trace", None)
    ):
        print("Error: Please specify an original job trace or a formatted job data!")
        p.print_help()
        sys.exit()
    if (
        not opts.node_struc
        and not opts.node_save
        and not inputPara.get("node_struc", None)
    ):
        print(
            "Error: Please specify an original node structure or a formatted node data!"
        )
        p.print_help()
        sys.exit()

    if not opts.alg and not inputPara["alg"]:
        print("Error: Please specify the algorithm element!")
        p.print_help()
        sys.exit()

    for key in inputPara:
        if opts.__dict__.get(key):
            inputPara[key] = opts.__dict__[key]
    for data_path in "path_in", "path_out", "path_fmt", "path_debug":
        assert (
            data_path in inputPara and inputPara[data_path]  # type: ignore
        ), f"Error: Please specify the {data_path}!"
        inputPara[data_path] = os.path.join(cqsim_path.path_data, inputPara[data_path])  # type: ignore

    inputPara["alg_sign"] = alg_sign_check(inputPara["alg_sign"], len(inputPara["alg"]))

    if inputPara.get("job_trace") is None:
        inputPara["resource_job"] = 1
    if not opts.node_struc:
        inputPara["resource_node"] = 1
    if inputPara.get("output") is None:
        inputPara["output"] = path_without_extension(opts.job_trace)
    if inputPara.get("debug") is None:
        inputPara["debug"] = "debug_" + path_without_extension(opts.job_trace)
    if inputPara.get("job_save") is None:
        inputPara["job_save"] = path_without_extension(opts.job_trace)
    if inputPara.get("node_save") is None:
        inputPara["node_save"] = path_without_extension(opts.job_trace) + "_node"
    """
    if not opts.job_save:
        print "Error: Please specify at least one node structure!"
        p.print_help()
        sys.exit()
    """

    cqsim_main.cqsim_main(inputPara)  # type: ignore
