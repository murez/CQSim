from itertools import permutations
from typing import Optional

from cqsim.cqsim.backfill import BackfillPara
from cqsim.cqsim.types import WaitInfo
from cqsim.extend.swf.node import NodeSWF
from cqsim.logging.debug import DebugLog
from cqsim.types import Time


class StartWindowPara:
    win_size: int
    check_size_in: int
    max_start_size: int

    def __init__(
        self,
        win_size: int = 1,
        check_size_in: Optional[int] = None,
        max_start_size: Optional[int] = None,
    ):
        self.win_size = win_size
        self.check_size_in = check_size_in or self.win_size
        self.max_start_size = max_start_size or self.win_size


class StartWindow:
    current_para: Optional[BackfillPara]
    wait_jobs: list[WaitInfo]
    para: StartWindowPara
    use_window: bool
    para_list_ad: Optional[list[int]] = None

    def __init__(
        self,
        mode: bool,  # False
        ad_mode: int,  # 0
        node_module: NodeSWF,
        debug: DebugLog,
        para_list: StartWindowPara = StartWindowPara(6, 0, 0),
        para_list_ad: Optional[list[int]] = None,
    ):
        self.display_name = "Start Window"
        self.use_window = mode
        self.ad_mode = ad_mode
        self.node_module = node_module
        self.debug = debug
        self.para = para_list
        self.para_list_ad = para_list_ad

        self.current_para = None

        self.debug.line(4, " ")
        self.debug.line(4, "#")
        self.debug.debug("# " + self.display_name, 1)
        self.debug.line(4, "#")

        self.reset_list()

    def reset(
        self,
        mode: Optional[bool] = None,
        ad_mode: Optional[int] = None,
        node_module: Optional[NodeSWF] = None,
        debug: Optional[DebugLog] = None,
        para_list: Optional[StartWindowPara] = None,
        para_list_ad: Optional[list[int]] = None,
    ):
        # self.debug.debug("* "+self.display_name+" -- reset",5)
        if mode:
            self.use_window = mode
        if ad_mode:
            self.ad_mode = ad_mode
        if node_module:
            self.node_module = node_module
        if debug:
            self.debug = debug
        if para_list:
            self.para = para_list
        if para_list_ad:
            self.para_list_ad = para_list_ad

        self.current_para = None
        self.reset_list()

    def start_window(
        self, wait_jobs: list[WaitInfo], para_in: Optional[BackfillPara] = None
    ):
        # self.debug.debug("* "+self.display_name+" -- start_window",5)
        self.current_para = para_in
        self.wait_jobs = wait_jobs[: self.para.win_size]
        check_len = min(self.para.check_size_in, len(self.wait_jobs))

        result = self.main(check_len)
        return result

    def main(self, check_len: int):
        # self.debug.debug("* "+self.display_name+" -- main",5)
        if self.use_window:
            return self.window_check(check_len)
        else:
            return [job.index for job in self.wait_jobs[:check_len]]

    def window_adapt(self, para_in: Optional[BackfillPara] = None):
        # self.debug.debug("* "+self.display_name+" -- window_adapt",5)
        return 0

    def window_size(self):
        # self.debug.debug("* "+self.display_name+" -- window_size",6)
        return self.para.win_size

    def check_size(self):
        # self.debug.debug("* "+self.display_name+" -- check_size",6)
        return self.para.check_size_in

    def start_num(self):
        # self.debug.debug("* "+self.display_name+" -- start_num",6)
        return self.para.max_start_size

    def reset_list(self):
        # self.debug.debug("* "+self.display_name+" -- reset_list",5)
        self.wait_jobs = []

    def window_check(self, check_len: int):
        # self.debug.debug("* "+self.display_name+" -- window_check",5)
        assert self.current_para is not None
        all_permutations = permutations(range(check_len))

        last_ended_wait_indices: list[int] = []
        last_ended: Optional[Time] = None

        if check_len == 1:
            return [self.wait_jobs[0].index]

        for wait_indices in all_permutations:
            self.node_module.predict_reset(self.current_para.time)
            wait_jobs = (self.wait_jobs[i] for i in wait_indices)

            index = 0
            for job in wait_jobs:
                index = self.node_module.reserve(
                    job.proc,
                    job.index,
                    job.run,
                    index=index,
                )

            end_time = self.node_module.predict_last_ended()
            if end_time is not None and (last_ended is None or last_ended < end_time):
                last_ended = self.node_module.predict_last_ended()
                last_ended_wait_indices = list(wait_indices)

        latest_end_job_indices = [
            self.wait_jobs[i].index for i in last_ended_wait_indices
        ]
        return latest_end_job_indices
