import bisect
from typing import Optional

from cqsim.cqsim.node import JobInfo, Node, PredictJob, PredictNode
from cqsim.types import Time


class NodeSWF(Node):
    def node_allocate(self, cores: int, job_index: int, start: Time, end: Time):
        # self.debug.debug("* "+self.display_name+" -- node_allocate",5)
        if not self.is_available(cores):
            return 0

        self.idle_cores -= cores
        self.available_cores = self.idle_cores

        # Insert job into jobs list, keeping it sorted by end time
        job_info = JobInfo(
            job=job_index,
            end=end,
            node=cores,
        )
        bisect.insort(self.jobs, job_info, key=lambda x: x.end)

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
        """
        self.debug.line(2,"...")
        for job in self.job_list:
            self.debug.debug(job['job'],2)
        self.debug.line(2,"...")
        """

        # find matching job in jobs list
        for index, job in enumerate(self.jobs):
            if job.job == job_index:
                job = self.jobs.pop(index)
                self.idle_cores += job.node
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

    def reserve(
        self,
        cores: int,
        job_index: int,
        time: Time,  # time period
        start: Optional[Time] = None,
        index: Optional[int] = None,
    ):
        # self.debug.debug("* "+self.display_name+" -- reserve",5)
        if index is None:
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
                else:  # cores > node.avali
                    index += 1
        elif not self.predict_avail(cores, start, start + time):
            return None

        assert start is not None
        end = start + time

        reserve_index = None
        for i, node in enumerate(self.predict_nodes, start=index):
            if node.time < end:
                node.idle -= cores
                node.avail = node.idle
            elif node.time == end:
                break
            else:
                last_node = self.predict_nodes[i - 1]
                node = PredictNode(
                    time=end,
                    idle=last_node.idle + cores,
                    avail=last_node.idle + cores,
                )

                self.predict_nodes.insert(
                    i,
                    node,
                )
                reserve_index = i
                break
        else:
            self.predict_nodes.append(
                PredictNode(time=end, idle=self.total, avail=self.total)
            )
            reserve_index = len(self.predict_nodes) - 1

        self.predict_job.append(PredictJob(job=job_index, start=start, end=end))
        return reserve_index

    def predict_reset(self, time: Time) -> None:
        # self.debug.debug("* "+self.display_name+" -- pre_reset",5)
        node = PredictNode(time=time, idle=self.idle_cores, avail=self.available_cores)
        self.predict_nodes = [node]
        self.predict_job = []

        for i, job in enumerate(self.jobs):
            if node.time != job.end or i == 0:
                node = PredictNode(
                    time=job.end,
                    idle=node.idle,
                    avail=node.avail,
                )
                self.predict_nodes.append(node)

            assert node.time == job.end
            node.idle += job.node
            node.avail = self.predict_nodes[-1].idle

    def find_res_place(self, cores: int, index: int, time: Time):
        """
        In self.predict_nodes[index:], find the index of the first node that has enough cores
        and is available for the given time.
        """
        # self.debug.debug("* "+self.display_name+" -- find_res_place",5)
        if index >= len(self.predict_nodes):
            index = len(self.predict_nodes) - 1

        end = self.predict_nodes[index].time + time

        for i, node in enumerate(self.predict_nodes, index):
            if node.time < end and cores > node.avail:
                return i
