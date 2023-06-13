from typing import Optional

from cqsim.cqsim.basic_algorithm import BasicAlgorithm
from cqsim.cqsim.types import NodeInfo
from cqsim.logging.debug import DebugLog
from cqsim.types import EventCode, Time


class InfoCollect:
    def __init__(self, alg_module: BasicAlgorithm, debug: DebugLog):
        self.display_name = "Info Collect"
        self.alg_module = alg_module
        self.debug = debug
        # self.sys_info = []

        self.debug.line(4, " ")
        self.debug.line(4, "#")
        self.debug.debug("# " + self.display_name, 1)
        self.debug.line(4, "#")

    def reset(
        self,
        alg_module: Optional[BasicAlgorithm] = None,
        debug: Optional[DebugLog] = None,
    ):
        self.debug.debug("* " + self.display_name + " -- reset", 5)
        if alg_module:
            self.alg_module = alg_module
        if debug:
            self.debug = debug
        # self.sys_info = []

    def info_collect(
        self,
        time: Time,
        event: Optional[EventCode],
        uti: float,
        wait_num: int = -1,
        wait_size: int = -1,
        inter: float = -1.0,
        extend: Optional[dict] = None,
    ):
        self.debug.debug("* " + self.display_name + " -- info_collect", 5)
        event_date = time
        temp_info: NodeInfo = {
            "date": event_date,
            "time": time,
            "event": event,
            "uti": uti,
            "waitNum": wait_num,
            "waitSize": wait_size,
            "inter": inter,
            "extend": extend,
        }
        self.debug.debug("   " + str(temp_info), 4)
        # self.sys_info.append(temp_info)
        return temp_info
