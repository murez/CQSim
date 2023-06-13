from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import NamedTuple, Optional, TypedDict

import pandas as pd

from cqsim.types import EventCode, Time


class EventType(Enum):
    JOB = 1
    MONITOR = 2
    EXTEND = 3
    """Specially designed for new requirement"""


class EventState(Enum):
    SUBMIT = 1
    FINISH = 2


class EventPara(NamedTuple):
    state: EventState
    job_index: int


@dataclass(eq=True)
class Event:
    type: EventType
    """The type of the event."""

    time: Time
    """Virtual time."""

    prio: int
    """Priority."""

    para: Optional[EventPara]
    """Event parameter list."""

    def _cmp_key(self):
        return (self.time, -self.prio, self.para, self.type)

    def __lt__(self, other: Event):
        return self._cmp_key() < other._cmp_key()

    def __le__(self, other: Event):
        return self._cmp_key() <= other._cmp_key()

    def __gt__(self, other: Event):
        return self._cmp_key() > other._cmp_key()

    def __ge__(self, other: Event):
        return self._cmp_key() >= other._cmp_key()


@dataclass
class WaitInfo:
    index: int
    proc: int
    node: int
    run: Time
    score: float


class NodeInfo(TypedDict):
    date: Time
    time: Time
    event: Optional[EventCode]
    uti: float
    waitNum: int
    waitSize: int
    inter: float
    extend: Optional[dict]


# TODO: -1 => None
# The Standard Workload Format (swf):
# https://www.cs.huji.ac.il/labs/parallel/workload/swf.html
@dataclass
class Job:
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


def scale_submit_time(
    jobs: list[Job] | pd.DataFrame, min_submit_time: float, density: float, start: float
):
    if isinstance(jobs, list):
        for job in jobs:
            job.submit_time = density * (job.submit_time - min_submit_time) + start
    else:
        jobs["submit_time"] = density * (jobs["submit_time"] - min_submit_time) + start


# https://stackoverflow.com/questions/61386477/type-hints-for-a-pandas-dataframe-with-mixed-dtypes
JobDataFrame = pd.DataFrame(
    columns=[
        "id",
        "submit_time",
        "wait_time",
        "run_time",
        "allocated_processors",
        "average_cpu_time",
        "used_memory",
        "requested_number_processors",
        "requested_time",
        "requested_memory",
        "status",
        "user_id",
        "group_id",
        "executable_number",
        "queue_number",
        "partition_number",
        "previous_job_id",
        "think_time_from_previous_job",
    ]
).astype(
    dtype={
        "id": int,
        "submit_time": float,
        "wait_time": float,
        "run_time": float,
        "allocated_processors": int,
        "average_cpu_time": float,
        "used_memory": float,
        "requested_number_processors": int,
        "requested_time": float,
        "requested_memory": float,
        "status": int,
        "user_id": int,
        "group_id": int,
        "executable_number": int,
        "queue_number": int,
        "partition_number": int,
        "previous_job_id": int,
        "think_time_from_previous_job": int,
    }
)
