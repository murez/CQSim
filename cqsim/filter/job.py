from dataclasses import dataclass
from typing import Optional

from cqsim.cqsim.job_trace import Job
from cqsim.IOModule.debug import DebugLog
from cqsim.types import Time


@dataclass
class ConfigData:
    date: str  # TODO: datetime
    start_offset: Time


class JobFilter:
    job_list: list[Job]
    start: Optional[Time]
    config_data: Optional[ConfigData]
    anchor: int

    def __init__(
        self,
        trace: str,
        save: str,
        config: str,
        sdate: Optional[Time] = None,
        start: Optional[Time] = None,
        density=1.0,
        anchor=0,
        rnum=0,
        debug: Optional[DebugLog] = None,
    ):
        self.display_name = "Filter Job"
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
        self.job_list = []

        if self.debug:
            self.debug.line(4, " ")
            self.debug.line(4, "#")
            self.debug.debug("# " + self.display_name, 1)
            self.debug.line(4, "#")

        self.reset_config()

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
        debug: Optional[DebugLog] = None,
    ):
        if self.debug:
            self.debug.debug("* " + self.display_name + " -- reset", 5)
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
        self.job_list = []

        self.reset_config()

    def reset_config(self):
        if self.debug:
            self.debug.debug("* " + self.display_name + " -- reset_config_data", 5)
        self.config_data = None

    def read_job_trace(self):
        if self.debug:
            self.debug.debug("* " + self.display_name + " -- read_job_trace", 5)
        raise NotImplementedError

    def input_check(self, job: Job):
        if self.debug:
            self.debug.debug("* " + self.display_name + " -- input_check", 5)
        raise NotImplementedError

    def get_job_num(self):
        if self.debug:
            self.debug.debug("* " + self.display_name + " -- get_job_num", 6)
        return self.jobNum

    def get_job_data(self):
        if self.debug:
            self.debug.debug("* " + self.display_name + " -- get_job_data", 5)
        return self.job_list

    def dump_job_list(self):
        if self.debug:
            self.debug.debug("* " + self.display_name + " -- output_job_data", 5)
        if not self.save:
            print("Save file not set!")
            return
        raise NotImplementedError

    def dump_config(self):
        if self.debug:
            self.debug.debug("* " + self.display_name + " -- output_job_config", 5)
        if not self.config:
            print("Config file not set!")
            return
        raise NotImplementedError
