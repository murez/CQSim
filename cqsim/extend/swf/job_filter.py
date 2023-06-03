import dataclasses
import json

import pandas as pd

from cqsim.cqsim import Job, JobTraceInfo
from cqsim.extend import swf
from cqsim.filter import JobFilter
from cqsim.filter.job import ConfigData
from cqsim.types import Time
from cqsim.utils import dataclass_types_for_pandas


class JobFilterSWF(JobFilter):
    def reset_config(self):
        self.config_data = None

    def feed_job_trace(self):
        if not self.save:
            print("Save file not set!")
            return

        with open(self.trace, "r") as f:
            jobs = swf.load(f)

        min_submit_time = jobs.jobs[0].submit_time
        if self.start is None:
            self.start = min_submit_time
        assert self.start >= 0

        self.config_data = ConfigData(
            date=jobs.headers["StartTime"],
            start_offset=min_submit_time - self.start,
        )

        filtered_jobs: list[Job] = []
        for job in jobs.jobs:
            job.scale_submit_time(min_submit_time, self.density, self.start)
            if self.input_check(job):
                filtered_jobs.append(job)

        df = pd.DataFrame([dataclasses.asdict(job) for job in filtered_jobs])
        df.to_csv(self.save, index=False)

        self.job_count = len(filtered_jobs)

    def read_job_trace(self):
        field_types = dataclass_types_for_pandas(Job)
        df = pd.read_csv(
            self.trace,
            dtype=field_types,
            comment=";",
        )

        # set job list
        for index, row in df.iterrows():
            job = Job(**row.to_dict())
            if self.input_check(job):
                self.job_list.append(job)

        # set job num
        self.job_count = len(self.job_list)

    def input_check(self, job: Job):
        if int(job.run_time) > int(job.requested_time):
            job.run_time = job.requested_time
        if int(job.id) <= 0:
            return False
        if int(job.submit_time) < 0:
            return False
        if int(job.run_time) <= 0:
            return False
        if int(job.requested_time) <= 0:
            return False
        if int(job.requested_number_processors) <= 0:
            return False
        return True

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
