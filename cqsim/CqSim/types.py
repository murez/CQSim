from typing import Optional, TypedDict

from cqsim.types import EventCode, Time


class Event(TypedDict):
    type: int
    time: Time
    prio: int
    para: Optional[list[int]]


class WaitInfo(TypedDict):
    index: int
    proc: int
    node: int
    run: float
    score: int


class NodeInfo(TypedDict):
    date: Time
    time: Time
    event: Optional[EventCode]
    uti: float
    waitNum: int
    waitSize: int
    inter: float
    extend: Optional[dict]
