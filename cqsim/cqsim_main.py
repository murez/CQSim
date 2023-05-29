import os
from typing import TypedDict

from typing_extensions import Required

# import CqSim.Node_struc as Class_Node_struc
from cqsim.cqsim.backfill import Backfill
from cqsim.cqsim.basic_algorithm import BasicAlgorithm
from cqsim.cqsim.cqsim import Cqsim, ModuleList
from cqsim.cqsim.info_collect import InfoCollect
from cqsim.cqsim.job_trace import JobTrace
from cqsim.cqsim.window import StartWindow
from cqsim.extend.swf.job_filter import JobFilterSWF
from cqsim.extend.swf.node import NodeSWF
from cqsim.extend.swf.node_filter import NodeFilterSWF
from cqsim.IOModule.debug import DebugLog
from cqsim.IOModule.output import Output, OutputLog


class ParaList(TypedDict):
    resource_job: int
    resource_node: int
    job_trace: str
    node_struc: str
    job_save: str
    node_save: str
    cluster_fraction: float
    start: float
    start_date: str
    anchor: int
    read_num: int
    pre_name: str
    output: str
    debug: str
    ext_fmt_j: str
    ext_fmt_n: str
    ext_fmt_j_c: str
    ext_fmt_n_c: str
    path_in: str
    path_out: str
    path_fmt: str
    path_debug: str
    ext_jr: str
    ext_si: str
    ext_ai: str
    ext_debug: str
    debug_lvl: int
    alg: Required[list[str]]
    alg_sign: Required[list[str]]
    backfill: int
    bf_para: Required[list[str]]
    win: int
    win_para: Required[list[int]]
    ad_win: int
    ad_win_para: Required[list[int]]
    ad_bf: int
    ad_bf_para: Required[list[str]]
    ad_alg: int
    ad_alg_para: Required[list[str]]
    config_n: str
    config_sys: str
    monitor: int
    log_freq: int
    read_input_freq: int


class OptionalParaList(TypedDict, total=False):
    resource_job: int
    resource_node: int
    job_trace: str
    node_struc: str
    job_save: str
    node_save: str
    cluster_fraction: float
    start: float
    start_date: str
    anchor: int
    read_num: int
    pre_name: str
    output: str
    debug: str
    ext_fmt_j: str
    ext_fmt_n: str
    ext_fmt_j_c: str
    ext_fmt_n_c: str
    path_in: str
    path_out: str
    path_fmt: str
    path_debug: str
    ext_jr: str
    ext_si: str
    ext_ai: str
    ext_debug: str
    debug_lvl: int
    alg: Required[list[str]]
    alg_sign: Required[list[str]]
    backfill: int
    bf_para: Required[list[str]]
    win: int
    win_para: Required[list[str]]
    ad_win: int
    ad_win_para: Required[list[str]]
    ad_bf: int
    ad_bf_para: Required[list[str]]
    ad_alg: int
    ad_alg_para: Required[list[str]]
    config_n: str
    config_sys: str
    monitor: int
    log_freq: int
    read_input_freq: int


def cqsim_main(para_list: ParaList):
    print("....................")
    for item in para_list:
        print(str(item) + ": " + str(para_list[item]))
    print("....................")

    trace_name = para_list["path_in"] + para_list["job_trace"]
    save_name_j = para_list["path_fmt"] + para_list["job_save"] + para_list["ext_fmt_j"]
    config_name_j = (
        para_list["path_fmt"] + para_list["job_save"] + para_list["ext_fmt_j_c"]
    )
    struc_name = para_list["path_in"] + para_list["node_struc"]
    save_name_n = (
        para_list["path_fmt"] + para_list["node_save"] + para_list["ext_fmt_n"]
    )
    config_name_n = (
        para_list["path_fmt"] + para_list["node_save"] + para_list["ext_fmt_n_c"]
    )

    output_sys = para_list["path_out"] + para_list["output"] + para_list["ext_si"]
    output_adapt = para_list["path_out"] + para_list["output"] + para_list["ext_ai"]
    output_result = para_list["path_out"] + para_list["output"] + para_list["ext_jr"]
    output_fn: Output = {
        "sys": output_sys,
        "adapt": output_adapt,
        "result": output_result,
    }
    log_freq_int = para_list["log_freq"]
    para_list["read_input_freq"]

    if not os.path.exists(para_list["path_fmt"]):
        os.makedirs(para_list["path_fmt"])

    if not os.path.exists(para_list["path_out"]):
        os.makedirs(para_list["path_out"])

    if not os.path.exists(para_list["path_debug"]):
        os.makedirs(para_list["path_debug"])

    # Debug
    print(".................... Debug")
    debug_path = para_list["path_debug"] + para_list["debug"] + para_list["ext_debug"]
    module_debug = DebugLog(
        lvl=para_list["debug_lvl"], show=2, path=debug_path, log_freq=log_freq_int
    )
    # module_debug.start_debug()

    # Job Filter
    print(".................... Job Filter")
    dirPath = os.path.dirname(save_name_j)
    if not os.path.exists(dirPath):
        os.makedirs(dirPath, exist_ok=True)
    module_filter_job = JobFilterSWF(
        trace=trace_name, save=save_name_j, config=config_name_j, debug=module_debug
    )
    module_filter_job.feed_job_trace()
    # module_filter_job.read_job_trace()
    # module_filter_job.output_job_data()
    module_filter_job.output_job_config()

    # Node Filter
    print(".................... Node Filter")
    dirPath = os.path.dirname(save_name_n)
    if not os.path.exists(dirPath):
        os.makedirs(dirPath, exist_ok=True)
    module_filter_node = NodeFilterSWF(
        struc=struc_name, save=save_name_n, config=config_name_n, debug=module_debug
    )
    module_filter_node.read_node_struc()
    module_filter_node.dump_node_list()
    module_filter_node.dump_config()

    # Job Trace
    print(".................... Job Trace")
    module_job_trace = JobTrace(
        start=para_list["start"],
        num=para_list["read_num"],
        anchor=para_list["anchor"],
        density=para_list["cluster_fraction"],
        read_input_freq=para_list["read_input_freq"],
        debug=module_debug,
    )
    module_job_trace.initial_import_job_file(save_name_j)
    # module_job_trace.import_job_file(save_name_j)
    module_job_trace.import_job_config(config_name_j)

    # Node Structure
    print(".................... Node Structure")
    module_node_struc = NodeSWF(debug=module_debug)
    module_node_struc.import_node_file(save_name_n)
    module_node_struc.import_node_config(config_name_n)

    # Backfill
    print(".................... Backfill")
    module_backfill = Backfill(
        ad_mode=0,
        mode=para_list["backfill"],
        node_module=module_node_struc,
        debug=module_debug,
        para_list=para_list["bf_para"],
    )

    # Start Window
    print(".................... Start Window")
    module_win = StartWindow(
        mode=para_list["win"],
        ad_mode=0,
        node_module=module_node_struc,
        debug=module_debug,
        para_list=para_list["win_para"],
        para_list_ad=para_list["ad_win_para"],
    )

    # Basic Algorithm
    print(".................... Basic Algorithm")
    module_alg = BasicAlgorithm(
        ad_mode=0,
        element=(para_list["alg"], para_list["alg_sign"]),
        debug=module_debug,
        paralist=para_list["ad_alg_para"],
    )

    # Information Collect
    print(".................... Information Collect")
    module_info_collect = InfoCollect(alg_module=module_alg, debug=module_debug)

    # Output Log
    print(".................... Output Log")
    module_output_log = OutputLog(output=output_fn, log_freq=log_freq_int)

    # Cqsim Simulator
    print(".................... Cqsim Simulator")
    module_list = ModuleList(
        job=module_job_trace,
        node=module_node_struc,
        backfill=module_backfill,
        win=module_win,
        alg=module_alg,
        info=module_info_collect,
        output=module_output_log,
    )

    module_sim = Cqsim(
        module=module_list, debug=module_debug, monitor=para_list["monitor"]
    )
    module_sim.cqsim_sim()
    # module_debug.end_debug()
