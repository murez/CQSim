from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import NamedTuple, Optional, TypedDict

from cqsim.types import EventCode, Time


class EventType(Enum):
    JOB = 1
    MONITOR = 2
    EXTEND = 3


class EventState(Enum):
    SUBMIT = 1
    FINISH = 2


class EventPara(NamedTuple):
    state: EventState
    job_index: int


@dataclass(eq=True)
class Event:
    type: EventType
    time: Time
    prio: int
    para: Optional[EventPara]

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

    def scale_submit_time(self, min_submit_time: float, density: float, start: float):
        self.submit_time = density * (self.submit_time - min_submit_time) + start
