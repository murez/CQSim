import bisect
import itertools
import json
from dataclasses import dataclass, field
from typing import Callable, Optional

import pandas as pd
from typing_extensions import reveal_type

from cqsim.logging.debug import DebugLog
from cqsim.types import StrOrBytesPath, Time
from cqsim.utils import dataclass_types_for_pandas


@dataclass
class JobInfo:
    job: int
    cores: int
    end: Time


@dataclass
class PredictNode:
    time: Time
    avail: int
    idle: int
    # A list of nodes, whose length is Node.total
    # TODO: what is this in the end?
    node: list[int] = field(default_factory=list)


@dataclass
class PredictJob:
    job: int
    start: Time
    end: Time


@dataclass
class NodeStructure:
    id: int
    location: list[int]
    group: int
    proc: int
    # index to the assigned job, or None if not assigned
    state: Optional[int] = None
    start: Optional[Time] = None
    end: Optional[Time] = None
    extend: Optional[dict] = None


class Node:
    """A class to store the node structure and node information.

    Attributes:
        nodes: A list of node structures.
        jobs: A list of job information, sorted by end time.
    """

    nodes: list[NodeStructure]
    jobs: list[JobInfo]
    predict_nodes: list[PredictNode]
    predict_jobs: list[PredictJob]

    # Count of processors
    total_cores: Optional[int] = None
    idle_cores: Optional[int] = None
    available_cores: Optional[int] = None

    def __init__(self, debug: DebugLog):
        self.display_name = "Node Structure"
        self.debug = debug
        self.nodes = []
        self.jobs = []
        self.predict_nodes = []
        self.predict_jobs = []
        self.total_cores = None
        self.idle_cores = None
        self.available_cores = None

        self.debug.line(4, " ")
        self.debug.line(4, "#")
        self.debug.debug("# " + self.display_name, 1)
        self.debug.line(4, "#")

    def reset(self, debug: DebugLog):
        # self.debug.debug("* "+self.display_name+" -- reset",5)
        self.debug = debug
        self.nodes = []
        self.jobs = []
        self.predict_nodes = []
        self.total_cores = None
        self.idle_cores = None
        self.available_cores = None

    def read_list(self, source_str: str):
        assert source_str[0] == "[" and source_str[-1] == "]"
        source_str = source_str[1:-1]
        splited = source_str.split(",")
        return [int(item) for item in splited]

    def import_node_file(self, node_file: str):
        # self.debug.debug("* "+self.display_name+" -- import_node_file",5)
        self.nodes = []

        field_types = dataclass_types_for_pandas(NodeStructure) | {
            "location": str,
            "extend": str,
        }
        df = pd.read_csv(
            node_file,
            dtype=field_types,
            comment=";",
        )
        df["location"] = df["location"].map(self.read_list)
        df["extend"] = pd.NA

        for index, row in df.iterrows():
            self.nodes.append(NodeStructure(*row.to_dict()))
        self.total_cores = len(self.nodes)
        self.idle_cores = self.total_cores
        self.available_cores = self.total_cores

        self.debug.debug(
            "  Tot:"
            + str(self.total_cores)
            + " Idle:"
            + str(self.idle_cores)
            + " Avail:"
            + str(self.available_cores)
            + " ",
            4,
        )
        return

    def import_node_config(self, config_file: StrOrBytesPath):
        with open(config_file, "r") as f:
            json.load(f)

    def import_node_data(self, node_data: list):
        # self.debug.debug("* "+self.display_name+" -- import_node_data",5)
        self.nodes = []

        temp_len = len(node_data)
        i = 0
        while i < temp_len:
            temp_dataList = node_data[i]

            tempInfo = NodeStructure(
                id=temp_dataList[0],
                location=temp_dataList[1],
                group=temp_dataList[2],
                state=temp_dataList[3],
                proc=temp_dataList[4],
                start=None,
                end=None,
                extend=None,
            )
            self.nodes.append(tempInfo)
            i += 1
        self.total_cores = len(self.nodes)
        self.idle_cores = self.total_cores
        self.available_cores = self.total_cores

    def is_available(self, cores: int):
        # self.debug.debug("* "+self.display_name+" -- is_available",6)
        if self.available_cores is None:
            self.debug.debug("  Error: Node is not initialized", 0)
            return False
        return self.available_cores >= cores

    def get_tot(self):
        # self.debug.debug("* "+self.display_name+" -- get_tot",6)
        assert self.total_cores is not None
        return self.total_cores

    def get_idle(self):
        # self.debug.debug("* "+self.display_name+" -- get_idle",6)
        assert self.idle_cores is not None
        return self.idle_cores

    def get_avail(self):
        # self.debug.debug("* "+self.display_name+" -- get_avail",6)
        assert self.available_cores is not None
        return self.available_cores

    def node_allocate(self, cores: int, job_index: int, start: Time, end: Time):
        assert self.idle_cores is not None

        # self.debug.debug("* "+self.display_name+" -- node_allocate",5)
        if not self.is_available(cores):
            return False

        # NOTE: this does not exists in NodeSWF
        assert all(node.state is not None for node in self.nodes)
        for node in itertools.islice(
            filter(
                lambda x: x.state < 0,  # type: ignore ensured all states are not None
                self.nodes,
            ),
            cores,
        ):
            node.state = job_index
            node.start = start
            node.end = end

        self.idle_cores -= cores
        self.available_cores = self.idle_cores
        job = JobInfo(job=job_index, end=end, cores=cores)
        bisect.insort(self.jobs, job, key=lambda x: x.end)

        self.debug.debug(
            "  Allocate"
            + "["
            + str(job_index)
            + "]"
            + " Req:"
            + str(cores)
            + " Avail:"
            + str(self.available_cores)
            + " ",
            4,
        )
        return 1

    def node_release(self, job_index: int, end: Time):
        # self.debug.debug("* "+self.display_name+" -- node_release",5)
        released_cores = 0
        for core in filter(lambda x: x.state == job_index, self.nodes):
            # self.debug.debug("  xxx: "+str(node['state'])+"   "+str(job_index),4)
            core.state = None
            core.start = None
            core.end = None
            released_cores += 1
        if released_cores == 0:
            self.debug.debug("  Release Fail!", 4)
            return False

        assert self.idle_cores is not None
        self.idle_cores += released_cores
        self.available_cores = self.idle_cores

        for index, job in enumerate(self.jobs):
            if job.job == job_index:
                job = self.jobs.pop(index)
                self.idle_cores += job.cores
                self.available_cores = self.idle_cores
                break
        else:
            raise ValueError(f"Job {job_index} not found in {self.jobs}")

        self.debug.debug(
            "  Release"
            + "["
            + str(job_index)
            + "]"
            + " Req:"
            + str(index)
            + " Avail:"
            + str(self.available_cores)
            + " ",
            4,
        )
        return True

    def predict_avail(self, cores: int, start: Time, end: Optional[Time] = None):
        """
        If the cores are available between start and end
        """
        # self.debug.debug("* "+self.display_name+" -- pre_avail",6)
        # self.debug.debug("pre avail check: "+str(proc_num)+" (" +str(start)+";"+str(end)+")",6)
        if not end or end < start:
            end = start

        between_time: Callable[[PredictNode], bool] = lambda x: start <= x.time < end
        nodes_between = filter(between_time, self.predict_nodes)
        return all(node.avail >= cores for node in nodes_between)

    def reserve(
        self,
        cores: int,
        job_index: int,
        time: Time,
        start: Optional[Time] = None,
        index: Optional[int] = None,
    ):
        assert self.total_cores is not None

        if index is None:  # in old code this means index == -1
            index = 0
        if index not in range(len(self.predict_nodes)):
            # return None
            raise ValueError(f"Index {index} not in range {len(self.predict_nodes)}")

        if start is None:
            while index < len(self.predict_nodes):
                node = self.predict_nodes[index]
                if cores <= node.avail:
                    if i := self.find_res_place(cores, index, time):
                        index = i + 1
                    else:
                        start = node.time
                        break
                else:
                    index += 1
        elif not self.predict_avail(cores, start, start + time):
            return None

        assert start
        end = start + time

        start_index = index
        for i, node in enumerate(self.predict_nodes, start=index):
            if node.time < end:
                assert len(node.node) == self.total_cores
                assigned_cores = 0
                for k, n in enumerate(node.node):
                    if assigned_cores >= cores:
                        break
                    if n != -1:
                        continue
                    node.node[k] = job_index
                    node.idle -= 1
                    node.avail = node.idle
                    assigned_cores += 1
            elif node.time == end:
                break
            else:  # node.time > end
                assert i > 0
                last_node = self.predict_nodes[i - 1]
                # And the last node must satisfy the condition
                assert last_node.time < end

                assert len(last_node.node) == self.total_cores
                self.predict_nodes.insert(
                    i,
                    PredictNode(
                        time=end,
                        # for list[int] shallow copy is enough
                        node=last_node.node.copy(),
                        idle=last_node.idle,
                        avail=last_node.avail,
                    ),
                )

                # rollback of operations on last_node
                assigned_cores = 0
                for k, n in enumerate(node.node):
                    if assigned_cores >= cores:
                        break
                    if n != job_index:
                        continue
                    node.node[k] = -1
                    node.idle += 1
                    node.avail = node.idle

                # self.debug.debug("xx   "+str(n)+"   "+str(k),4)
                break
        else:  # all nodes are before end
            self.predict_nodes.append(
                PredictNode(
                    time=end,
                    node=[-1] * self.total_cores,
                    idle=self.total_cores,
                    avail=self.total_cores,
                )
            )

        self.predict_jobs.append(PredictJob(job=job_index, start=start, end=end))
        return start_index

    def predict_delete(self, cores: int, job_index: int):
        # self.debug.debug("* "+self.display_name+" -- pre_delete",5)
        raise NotImplementedError

    def predict_modify(self, cores: int, start: Time, end: Time, job_index: int):
        # self.debug.debug("* "+self.display_name+" -- pre_modify",5)
        raise NotImplementedError

    def predict_last_start(self):
        if len(self.predict_jobs) == 0:
            return None
        return max(job.start for job in self.predict_jobs)

    def predict_last_ended(self):
        if len(self.predict_jobs) == 0:
            return None
        return max(job.end for job in self.predict_jobs)

    def predict_reset(self, time: Time):
        # self.debug.debug("* "+self.display_name+" -- pre_reset",5)
        assert len(self.nodes) == self.total_cores
        assert self.idle_cores is not None and self.available_cores is not None
        assert all(x.state is not None for x in self.nodes)

        node = PredictNode(
            time=time,
            node=[x.state for x in self.nodes],  # type: ignore ensured all states are not None
            idle=self.idle_cores,
            avail=self.available_cores,
        )
        self.predict_nodes = [node]
        self.predict_jobs = []
        self.predict_nodes.append(node)

        for i, job in enumerate(self.jobs):
            if node.time != job.end or i == 0:
                node = PredictNode(
                    time=job.end,
                    node=node.node.copy(),
                    idle=node.idle,
                    avail=node.avail,
                )
                self.predict_nodes.append(node)

            for k, n in enumerate(node.node):
                if n == job.job:
                    node.node[k] = -1
                    node.idle += 1
            node.avail = node.idle
        """
        i = 0
        while i< self.tot:
            if self.nodeStruc[i]['state'] != -1:
                temp_index = get_pre_index(temp_time = self.nodeStruc[i]['end'])
                self.predict_node[temp_index]['node'][i] = self.nodeStruc[i]['state']
            i += 1
        """
        return 1

    def find_res_place(self, cores: int, index: int, time: Time):
        """
        In predict_nodes[index:] which ends before time, find the index of the first node
        that DOES NOT have enough cores for the job.
        """
        self.debug.debug("* " + self.display_name + " -- find_res_place", 5)
        if index >= len(self.predict_nodes):
            index = len(self.predict_nodes) - 1

        end = self.predict_nodes[index].time + time

        for i, node in enumerate(self.predict_nodes[index:]):
            if node.time < end and cores > node.avail:
                return i
