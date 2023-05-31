from typing import Optional

from cqsim.cqsim.job_trace import JobTraceInfo
from cqsim.logging.debug import DebugLog
from cqsim.types import Time


class BasicAlgorithm:
    score_list: list[float]
    algorithm_expr: str

    def __init__(
        self,
        ad_mode: int,  # = 0,
        element: tuple[list[str], list[str]],
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
        ad_paralist=None,
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
        self, wait_jobs: list[JobTraceInfo], current_time: Time, para_list=None
    ):
        # self.debug.debug("* "+self.display_name+" -- get_score",5)
        self.score_list = []
        if len(wait_jobs) == 0:
            return self.score_list

        min_passed_time = min(current_time - job["submit"] for job in wait_jobs)
        min_request_time = min(job["reqTime"] for job in wait_jobs)
        if min_passed_time == 0:
            min_passed_time = 1

        for job in wait_jobs:
            submit = float(job["submit"])
            request_time = float(job["reqTime"])
            request_processors = float(job["reqProc"])
            passed_time = int(current_time - submit)
            # For backward compatibility
            z, l = min_passed_time, min_request_time
            s, t, n, w = submit, request_time, request_processors, passed_time
            self.score_list.append(float(eval(self.algorithm_expr)))

        # Prevent autoflake auto delete unused code
        if False:
            submit or request_time
            request_processors or passed_time or min_passed_time or min_request_time
            z or l or s or t or n or w

        # self.debug.debug("  Score:"+str(self.scoreList),4)
        return self.score_list

    def log_analysis(self):
        # self.debug.debug("* "+self.display_name+" -- log_analysis",5)
        return 1

    def alg_adapt(self, para_in):
        # self.debug.debug("* "+self.display_name+" -- alg_adapt",5)
        return 1
