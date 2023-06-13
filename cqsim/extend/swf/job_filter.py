import dataclasses
import json
from datetime import datetime

import pandas as pd
from pandas import DataFrame, Series

from cqsim.cqsim import Job, JobTraceInfo
from cqsim.cqsim.types import scale_submit_time
from cqsim.extend import swf
from cqsim.filter import JobFilter
from cqsim.filter.job import ConfigData
from cqsim.types import Time
from cqsim.utils import dataclass_types_for_pandas


def get_start_time(headers: dict[str, str]) -> str:
    if "StartTime" in headers:
        return headers["StartTime"]

    if "UnixStartTime" in headers:
        return datetime.fromtimestamp(int(headers["UnixStartTime"])).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    raise ValueError("Cannot find start time in headers!")


class JobFilterSWF(JobFilter):
    def reset_config(self):
        self.config_data = None

    def feed_job_trace(self):
        if not self.save:
            print("Save file not set!")
            return

        with open(self.trace, "r") as f:
            headers = swf.load_header(f)
            jobs = swf.load_jobs_df(f, swf=True)

        min_submit_time: Time = jobs.submit_time.iloc[0]
        if self.start is None:
            self.start = min_submit_time
        assert self.start >= 0

        self.config_data = ConfigData(
            date=get_start_time(headers),
            start_offset=min_submit_time - self.start,
        )

        scale_submit_time(jobs, min_submit_time, self.density, self.start)
        filtered_jobs = self.filter_input(jobs)
        filtered_jobs.to_csv(self.save, index=False)

        self.job_count = len(filtered_jobs)

    def read_job_trace(self):
        self.job_list = list(swf.load_jobs(self.trace, swf=True))
        # set job num
        self.job_count = len(self.job_list)

    def filter_input(self, jobs: DataFrame):
        # It's possible that a job runs longer than requested time
        # because scheduler will try to kill the job when it exceeds
        # the requested time.
        # However, the killing process is not immediate, and here we
        # want to ignore the killing process and only consider the
        # requested time.
        jobs["run_time"] = jobs[["run_time", "requested_time"]].min(axis=1)
        return jobs.query(
            "id > 0 and submit_time >= 0 and run_time > 0 and requested_time > 0 and requested_number_processors > 0"
        )

    def dump_job_list(self):
        df = pd.DataFrame([dataclasses.asdict(job) for job in self.job_list])
        df.to_csv(self.save, index=False)

    def dump_config(self):
        if not self.config:
            print("Config file not set!")
            return
        if not self.config_data:
            print("Config data not set!")
            return

        with open(self.config, "w") as f:
            json.dump(dataclasses.asdict(self.config_data), f)
