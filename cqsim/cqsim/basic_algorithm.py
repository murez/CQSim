from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from cqsim.cqsim.job_trace import JobTraceInfo
from cqsim.logging.debug import DebugLog
from cqsim.types import Time

if TYPE_CHECKING:
    from cqsim.cqsim_main import ParaList


class BasicAlgorithm:
    score_list: list[float]
    algorithm_expr: str

    def __init__(
        self,
        ad_mode: int,  # = 0,
        element: tuple[list[str], list[int]],
        debug: DebugLog,
        paralist: Optional[list[str]] = None,
        ad_paralist: Optional[str] = None,
    ):
        self.display_name = "Basic Algorithm"
        self.ad_mode = ad_mode
        self.element = element
        self.debug = debug
        self.paralist = paralist
        self.ad_paralist = ad_paralist

        self.debug.line(4, " ")
        self.debug.line(4, "#")
        self.debug.debug("# " + self.display_name, 1)
        self.debug.line(4, "#")

        self.score_list = []
        self.algorithm_expr = "".join(self.element[0])

    def reset(
        self,
        ad_mode: Optional[int] = None,
        element: Optional[list[str]] = None,
        debug: Optional[DebugLog] = None,
        paralist: Optional[str] = None,
        ad_paralist: Optional[str] = None,
    ):
        # self.debug.debug("* "+self.display_name+" -- reset",5)
        if ad_mode:
            self.ad_mode = ad_mode
        if element:
            self.element = element
        if debug:
            self.debug = debug
        if paralist:
            self.paralist = paralist
        if ad_paralist:
            self.ad_paralist = ad_paralist

        self.score_list = []
        self.algorithm_expr = "".join(self.element[0])

    def get_score(
        self,
        wait_jobs: list[JobTraceInfo],
        current_time: Time,
        para_list: Optional[ParaList] = None,
    ):
        # self.debug.debug("* "+self.display_name+" -- get_score",5)
        self.score_list = []
        if len(wait_jobs) == 0:
            return self.score_list

        max_passed_time = max(current_time - job.submit_time for job in wait_jobs)
        min_request_time = min(job.requested_time for job in wait_jobs)
        if max_passed_time == 0:
            max_passed_time = 1

        for job in wait_jobs:
            submit = float(job.submit_time)
            request_time = float(job.requested_time)
            request_processors = float(job.requested_number_processors)
            passed_time = int(current_time - submit)
            # For backward compatibility
            z, l = max_passed_time, min_request_time
            s, t, n, w = submit, request_time, request_processors, passed_time
            self.score_list.append(float(eval(self.algorithm_expr)))

        # Prevent autoflake auto delete unused code
        if False:
            submit or request_time
            request_processors or passed_time or max_passed_time or min_request_time
            z or l or s or t or n or w

        # self.debug.debug("  Score:"+str(self.scoreList),4)
        return self.score_list

    def log_analysis(self):
        # self.debug.debug("* "+self.display_name+" -- log_analysis",5)
        return 1

    def alg_adapt(self, para_in: Optional[list[int]]):
        # self.debug.debug("* "+self.display_name+" -- alg_adapt",5)
        return 1
