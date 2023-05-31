import dataclasses
import json
from typing import Optional, TypedDict

import pandas as pd

from cqsim.cqsim.types import Job
from cqsim.extend import swf
from cqsim.extend.swf.format import SWFLoader
from cqsim.filter.job import ConfigData
from cqsim.logging.debug import DebugLog
from cqsim.types import Time


class JobTraceInfo(TypedDict):
    id: int
    submit: float
    wait: float
    run: float
    usedProc: int
    usedAveCPU: float
    usedMem: float
    reqProc: int
    reqTime: float
    reqMem: float
    status: int
    userID: int
    groupID: int
    num_exe: int
    num_queue: int
    num_part: int
    num_pre: int
    thinkTime: int
    start: int  # -1
    end: int  # -1
    score: float  # 0
    state: int  # 0
    happy: int  # -1
    estStart: int  # -1


class JobTrace:
    traces: dict[int, JobTraceInfo]

    wait_indices: list[int]
    submit_indices: list[int]
    run_indices: list[int]

    min_submit_time: Optional[Time]
    swf_loader: SWFLoader

    def __init__(
        self,
        start: Time,  # -1
        num: Optional[int],  # -1
        anchor: int,  # -1
        density: float,  # 1.0
        read_input_freq: int,  # 1000
        debug: DebugLog,
    ):
        self.display_name = "Job Trace"
        self.start = start
        self.start_offset_A = 0.0
        self.start_offset_B = 0.0
        self.start_date = ""
        self.anchor = anchor
        self.read_num: int | None = num
        self.density = density
        self.debug = debug
        self.traces = dict()
        self.read_input_freq = read_input_freq
        self.num_delete_jobs = 0

        self.debug.line(4, " ")
        self.debug.line(4, "#")
        self.debug.debug("# " + self.display_name, 1)
        self.debug.line(4, "#")

        self.reset_data()

    def reset(
        self,
        start: Optional[int] = None,
        num: Optional[int] = None,
        anchor: Optional[int] = None,
        density: Optional[float] = None,
        read_input_freq: Optional[int] = None,
        debug: Optional[DebugLog] = None,
    ):
        # self.debug.debug("* "+self.display_name+" -- reset",5)
        if start:
            self.anchor = start
            self.start_offset_A = 0.0
            self.start_offset_B = 0.0
        if num:
            self.read_num = num
        if anchor:
            self.anchor = anchor
        if density:
            self.density = density
        if debug:
            self.debug = debug
        if read_input_freq:
            self.read_input_freq = read_input_freq
        self.traces = dict()
        self.reset_data()

    def reset_data(self):
        # self.debug.debug("* "+self.display_name+" -- reset_data",5)
        self.job_wait_size = 0
        self.submit_indices = []
        self.wait_indices = []
        self.run_indices = []
        # self.job_done_list=[]
        self.num_delete_jobs = 0

    # TODO: read file is still a mess

    def import_job_file(self, job_file: str):
        field_types = {field.name: field.type for field in dataclasses.fields(Job)}
        df = pd.read_csv(
            job_file,
            dtype=field_types,
            comment=";",
        )

        # set job list
        for index, row in df.iterrows():
            self.traces[len(self.traces)] = JobTraceInfo(
                start=-1,
                end=-1,
                score=0,
                state=0,
                happy=-1,
                estStart=-1,
                **row.to_dict()
            )

    # XREF: JobFilterSWF.output_job_config()
    def import_job_config(self, config_file):
        with open(config_file, "r") as f:
            config_data = ConfigData(**json.load(f))

        self.start_offset_A = config_data.start_offset
        self.start_date = config_data.date

    def submit_list(self):
        # self.debug.debug("* "+self.display_name+" -- submit_list",6)
        return self.submit_indices

    def wait_list(self):
        # self.debug.debug("* "+self.display_name+" -- wait_list",6)
        return self.wait_indices

    def run_list(self):
        # self.debug.debug("* "+self.display_name+" -- run_list",6)
        return self.run_indices

    """
    def done_list (self):
        #self.debug.debug("* "+self.display_name+" -- done_list",6)
        return self.job_done_list
    """

    def wait_size(self):
        # self.debug.debug("* "+self.display_name+" -- wait_size",6)
        return self.job_wait_size

    def refresh_score(self, scores: list[float]):
        # self.debug.debug("* "+self.display_name+" -- refresh_score",5)
        for index, score in zip(self.wait_indices, scores):
            self.traces[index]["score"] = score
        self.wait_indices.sort(key=lambda x: self.traces[x]["score"], reverse=True)

    def job_infos(self):
        return self.traces

    def job_info(self, job_index):
        # self.debug.debug("* "+self.display_name+" -- job_info",6)
        assert job_index >= 0
        return self.traces[job_index]

    def job_info_len(self):
        return len(self.traces) + self.num_delete_jobs

    def job_submit(self, job_index: int, job_score: int = 0, job_est_start: int = -1):
        # self.debug.debug("* "+self.display_name+" -- job_submit",5)
        self.traces[job_index]["state"] = 1
        self.traces[job_index]["score"] = job_score
        self.traces[job_index]["estStart"] = job_est_start
        self.submit_indices.remove(job_index)
        self.wait_indices.append(job_index)
        self.job_wait_size += self.traces[job_index]["reqProc"]
        return 1

    def job_start(self, job_index, time):
        # self.debug.debug("* "+self.display_name+" -- job_start",5)
        self.debug.debug(
            " "
            + "["
            + str(job_index)
            + "]"
            + " Req:"
            + str(self.traces[job_index]["reqProc"])
            + " Run:"
            + str(self.traces[job_index]["run"])
            + " ",
            4,
        )
        self.traces[job_index]["state"] = 2
        self.traces[job_index]["start"] = time
        self.traces[job_index]["wait"] = time - self.traces[job_index]["submit"]
        self.traces[job_index]["end"] = time + self.traces[job_index]["run"]
        self.wait_indices.remove(job_index)
        self.run_indices.append(job_index)
        self.job_wait_size -= self.traces[job_index]["reqProc"]
        return 1

    def job_finish(self, job_index, time=None):
        # self.debug.debug("* "+self.display_name+" -- job_finish",5)
        self.debug.debug(
            " "
            + "["
            + str(job_index)
            + "]"
            + " Req:"
            + str(self.traces[job_index]["reqProc"])
            + " Run:"
            + str(self.traces[job_index]["run"])
            + " ",
            4,
        )
        self.traces[job_index]["state"] = 3
        if time:
            self.traces[job_index]["end"] = time
        self.run_indices.remove(job_index)
        # self.job_done_list.append(job_index)
        return 1

    def job_set_score(self, job_index, job_score):
        self.traces[job_index]["score"] = job_score

    def remove_job_from_dict(self, job_index):
        # TODO: so it must be a dict right?
        self.num_delete_jobs += 1
        return self.traces.pop(job_index)
