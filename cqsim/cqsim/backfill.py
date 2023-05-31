"""
Backfilling module for CQSim
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from cqsim.cqsim.types import WaitInfo
from cqsim.extend.swf.node import NodeSWF
from cqsim.logging.debug import DebugLog
from cqsim.types import Time


class BackfillMode(Enum):
    EASY = 1
    CONSERVATIVE = 2


@dataclass
class BackfillPara:
    time: Time


class Backfill:
    display_name: str
    mode: BackfillMode
    ad_mode: int
    node: NodeSWF
    logger: DebugLog
    parameters: list[str]

    backfill_parameter: Optional[BackfillPara] = None
    wait_job: list[WaitInfo] = []

    def __init__(
        self,
        mode: int,  # 0
        ad_mode: int,  # 0
        node_module: NodeSWF,
        debug: DebugLog,
        para_list: list[str],
    ):
        self.display_name = "Backfill"
        self.mode = BackfillMode(value=mode)
        self.ad_mode = ad_mode
        self.node = node_module
        self.logger = debug
        self.parameters = para_list
        self.backfill_parameter = None
        self.wait_job = []

        self.logger.line(4, " ")
        self.logger.line(4, "#")
        self.logger.debug("# " + self.display_name, 1)
        self.logger.line(4, "#")

    def reset(
        self,
        mode: Optional[int] = None,
        ad_mode: Optional[int] = None,
        node_module: Optional[NodeSWF] = None,
        debug: Optional[DebugLog] = None,
        para_list: Optional[list[str]] = None,
    ):
        # self.debug.debug("* "+self.display_name+" -- reset",5)
        if mode:
            self.mode = BackfillMode(mode)
        if ad_mode:
            self.ad_mode = ad_mode
        if node_module:
            self.node = node_module
        if debug:
            self.logger = debug
        if para_list:
            self.parameters = para_list
        self.backfill_parameter = None
        self.wait_job = []

    def backfill(
        self, wait_job: list[WaitInfo], para_in: Optional[BackfillPara] = None
    ):
        # self.debug.debug("* "+self.display_name+" -- backfill",5)
        if len(wait_job) <= 1:
            return []
        self.backfill_parameter = para_in
        self.wait_job = wait_job
        job_list = self.main()
        return job_list

    def main(self):
        # self.debug.debug("* "+self.display_name+" -- main",5)
        result = []
        if self.mode == BackfillMode.EASY:
            # EASY backfill
            result = self.backfill_EASY()
        elif self.mode == BackfillMode.CONSERVATIVE:
            # Conservative backfill
            result = self.backfill_cons()
        else:
            return None
        return result

    def backfill_EASY(self):
        # self.debug.debug("* "+self.display_name+" -- backfill_EASY",5)
        backfill_list: list[int] = []
        assert self.backfill_parameter is not None
        self.node.predict_reset(self.backfill_parameter.time)
        self.node.reserve(
            self.wait_job[0].proc, self.wait_job[0].index, self.wait_job[0].run
        )

        for job in self.wait_job[1:]:
            backfill_test = self.node.predict_avail(
                job.proc,
                self.backfill_parameter.time,
                self.backfill_parameter.time + job.run,
            )
            if backfill_test:
                backfill_list.append(job.index)
                self.node.reserve(
                    job.proc,
                    job.index,
                    job.run,
                )
        return backfill_list

    def backfill_cons(self):
        # self.debug.debug("* "+self.display_name+" -- backfill_cons",5)
        backfill_list = []
        assert isinstance(self.backfill_parameter, dict)
        self.node.predict_reset(self.backfill_parameter["time"])
        self.node.reserve(
            self.wait_job[0].proc, self.wait_job[0].index, self.wait_job[0].run
        )

        for job in self.wait_job[1:]:
            backfill_test = self.node.predict_avail(
                job.proc,
                self.backfill_parameter["time"],
                self.backfill_parameter["time"] + job.run,
            )
            if backfill_test:
                backfill_list.append(job.index)
            self.node.reserve(
                job.proc,
                job.index,
                job.run,
            )
        return backfill_list
