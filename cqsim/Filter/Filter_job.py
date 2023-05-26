from typing import Optional

from cqsim.CqSim.Job_trace import JobTraceInfo
from cqsim.IOModule.Debug_log import Debug_log
from cqsim.types import Time


class Filter_job:
    jobList: list[JobTraceInfo]

    def __init__(
        self,
        trace: str,
        save: str,
        config: str,
        sdate: Optional[Time] = None,
        start: Time = -1,
        density=1.0,
        anchor=0,
        rnum=0,
        debug: Optional[Debug_log] = None,
    ):
        self.myInfo = "Filter Job"
        self.start = start
        self.sdate = sdate
        self.density = float(density)
        self.anchor = int(anchor)
        self.rnum = int(rnum)
        self.trace = str(trace)
        self.save = str(save)
        self.config = str(config)
        self.debug = debug
        self.jobNum = -1
        self.jobList = []

        if self.debug:
            self.debug.line(4, " ")
            self.debug.line(4, "#")
            self.debug.debug("# " + self.myInfo, 1)
            self.debug.line(4, "#")

        self.reset_config_data()

    def reset(
        self,
        trace: Optional[str] = None,
        save: Optional[str] = None,
        config: Optional[str] = None,
        sdate: Optional[Time] = None,
        start: Optional[Time] = None,
        density: Optional[float] = None,
        anchor: Optional[int] = None,
        rnum: Optional[int] = None,
        debug: Optional[Debug_log] = None,
    ):
        if self.debug:
            self.debug.debug("* " + self.myInfo + " -- reset", 5)
        if start:
            self.start = start
        if sdate:
            self.sdate = sdate
        if density:
            self.density = float(density)
        if anchor:
            self.anchor = int(anchor)
        if rnum:
            self.rnum = int(rnum)
        if trace:
            self.trace = str(trace)
        if save:
            self.save = str(save)
        if config:
            self.config = str(config)
        if debug:
            self.debug = debug
        self.jobNum = -1
        self.jobList = []

        self.reset_config_data()

    def reset_config_data(self):
        if self.debug:
            self.debug.debug("* " + self.myInfo + " -- reset_config_data", 5)
        self.config_start = ";"
        self.config_sep = "\\n"
        self.config_equal = ": "
        self.config_data = []
        # self.config_data.append({'name_config':'date','name':'StartTime','value':''})

    def read_job_trace(self):
        if self.debug:
            self.debug.debug("* " + self.myInfo + " -- read_job_trace", 5)
        return

    def input_check(self, jobInfo: JobTraceInfo):
        if self.debug:
            self.debug.debug("* " + self.myInfo + " -- input_check", 5)
        return

    def get_job_num(self):
        if self.debug:
            self.debug.debug("* " + self.myInfo + " -- get_job_num", 6)
        return self.jobNum

    def get_job_data(self):
        if self.debug:
            self.debug.debug("* " + self.myInfo + " -- get_job_data", 5)
        return self.jobList

    def output_job_data(self):
        if self.debug:
            self.debug.debug("* " + self.myInfo + " -- output_job_data", 5)
        if not self.save:
            print("Save file not set!")
            return
        return

    def output_job_config(self):
        if self.debug:
            self.debug.debug("* " + self.myInfo + " -- output_job_config", 5)
        if not self.config:
            print("Config file not set!")
            return
        return