import json
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pandas as pd

from cqsim.cqsim.types import Job
from cqsim.extend import swf
from cqsim.extend.swf.format import SWFLoader
from cqsim.filter.job import ConfigData
from cqsim.logging.debug import DebugLog
from cqsim.types import Time
from cqsim.utils import dataclass_types_for_pandas


class JobState(Enum):
    NONE = 0
    SUBMIT = 1
    START = 2
    FINISH = 3


@dataclass
class JobTraceInfo:
    id: int
    submit_time: float
    wait_time: float
    run_time: float
    allocated_processors: int
    average_cpu_time: float
    used_memory: float
    requested_number_processors: int
    requested_time: float
    requested_memory: float
    status: int
    user_id: int
    group_id: int
    executable_number: int
    queue_number: int
    partition_number: int
    previous_job_id: int
    think_time_from_previous_job: int
    start_time: Time = -1
    end_time: Time = -1
    score: float = 0
    state: JobState = JobState.NONE
    happy: int = -1
    estimated_start_time: Optional[Time] = None

    @classmethod
    def from_job(cls, job: Job):
        return cls(
            id=job.id,
            submit_time=job.submit_time,
            wait_time=job.wait_time,
            run_time=job.run_time,
            allocated_processors=job.allocated_processors,
            average_cpu_time=job.average_cpu_time,
            used_memory=job.used_memory,
            requested_number_processors=job.requested_number_processors,
            requested_time=job.requested_time,
            requested_memory=job.requested_memory,
            status=job.status,
            user_id=job.user_id,
            group_id=job.group_id,
            executable_number=job.executable_number,
            queue_number=job.queue_number,
            partition_number=job.partition_number,
            previous_job_id=job.previous_job_id,
            think_time_from_previous_job=job.think_time_from_previous_job,
        )


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
        self.read_count: int | None = num
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
            self.read_count = num
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
        self.job_wait_cores = 0
        self.submit_indices = []
        self.wait_indices = []
        self.run_indices = []
        # self.job_done_list=[]
        self.num_delete_jobs = 0

    # TODO: read file is still a mess

    def import_job_file(self, job_file: str):
        assert self.anchor >= 0
        # read job file
        jobs = swf.load_jobs(
            job_file, swf=False, skiprows=self.anchor, nrows=self.read_count
        )
        # set job list
        for job in jobs:
            self.traces[len(self.traces)] = JobTraceInfo.from_job(job)
            self.submit_indices.append(len(self.traces) - 1)

    # XREF: JobFilterSWF.output_job_config()
    def import_job_config(self, config_file: str):
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
        return self.job_wait_cores

    def refresh_score(self, scores: list[float]):
        # self.debug.debug("* "+self.display_name+" -- refresh_score",5)
        for index, score in zip(self.wait_indices, scores):
            self.traces[index].score = score
        self.wait_indices.sort(key=lambda x: self.traces[x].score, reverse=True)

    def job_infos(self):
        return self.traces

    def job_info(self, job_index: int):
        # self.debug.debug("* "+self.display_name+" -- job_info",6)
        assert job_index >= 0
        return self.traces[job_index]

    def job_info_len(self):
        return len(self.traces) + self.num_delete_jobs

    def job_submit(
        self, job_index: int, job_score: int = 0, job_est_start: Optional[Time] = None
    ):
        # self.debug.debug("* "+self.display_name+" -- job_submit",5)
        self.traces[job_index].state = JobState.SUBMIT
        self.traces[job_index].score = job_score
        self.traces[job_index].estimated_start_time = job_est_start
        self.submit_indices.remove(job_index)
        self.wait_indices.append(job_index)
        self.job_wait_cores += self.traces[job_index].requested_number_processors

    def job_start(self, job_index: int, time: Time):
        # self.debug.debug("* "+self.display_name+" -- job_start",5)
        self.debug.debug(
            " "
            + "["
            + str(job_index)
            + "]"
            + " Req:"
            + str(self.traces[job_index].requested_number_processors)
            + " Run:"
            + str(self.traces[job_index].run_time)
            + " ",
            4,
        )
        self.traces[job_index].state = JobState.START
        self.traces[job_index].start_time = time
        self.traces[job_index].wait_time = time - self.traces[job_index].submit_time
        self.traces[job_index].end_time = time + self.traces[job_index].run_time
        self.wait_indices.remove(job_index)
        self.run_indices.append(job_index)
        self.job_wait_cores -= self.traces[job_index].requested_number_processors

    def job_finish(self, job_index: int, time: Optional[Time] = None):
        """Mark a job as finished. Also removes it from the run queue."""
        self.debug.debug(
            " "
            + "["
            + str(job_index)
            + "]"
            + " Req:"
            + str(self.traces[job_index].requested_number_processors)
            + " Run:"
            + str(self.traces[job_index].run_time)
            + " ",
            4,
        )
        self.traces[job_index].state = JobState.FINISH
        if time:
            self.traces[job_index].end_time = time
        self.run_indices.remove(job_index)
        # self.job_done_list.append(job_index)
        return 1

    def job_set_score(self, job_index: int, job_score: float):
        self.traces[job_index].score = job_score

    def remove_job_from_dict(self, job_index: int):
        # TODO: so it must be a dict right?
        self.num_delete_jobs += 1
        return self.traces.pop(job_index)
