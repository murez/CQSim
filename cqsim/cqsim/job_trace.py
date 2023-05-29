import re
from io import TextIOWrapper
from typing import IO, Optional, TypedDict

from cqsim.extend.swf.format import SWFLoader
from cqsim.IOModule.debug import DebugLog
from cqsim.types import Time


# TODO: -1 => None
# The Standard Workload Format (swf):
# https://www.cs.huji.ac.il/labs/parallel/workload/swf.html
class Job(TypedDict):
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


class JobTraceInfo(TypedDict):
    id: int
    submit: float
    wait: float
    run: float
    usedProc: int
    usedAveCPU: float
    usedMem: float
    reqProc: int
    reqTime: float
    reqMem: float
    status: int
    userID: int
    groupID: int
    num_exe: int
    num_queue: int
    num_part: int
    num_pre: int
    thinkTime: int
    start: int  # -1
    end: int  # -1
    score: float  # 0
    state: int  # 0
    happy: int  # -1
    estStart: int  # -1


class JobTrace:
    file: Optional[TextIOWrapper]
    traces: list[JobTraceInfo]

    wait_indices: list[int]
    submit_indices: list[int]
    run_indices: list[int]

    min_submit_time: Optional[Time]
    swf_loader: SWFLoader

    def __init__(
        self,
        start: int,  # -1
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
        self.read_num: int | None = num
        self.density = density
        self.debug = debug
        self.traces = []
        self.file = None
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
            self.read_num = num
        if anchor:
            self.anchor = anchor
        if density:
            self.density = density
        if debug:
            self.debug = debug
        if read_input_freq:
            self.read_input_freq = read_input_freq
        self.traces = []
        self.file = None
        self.reset_data()

    def reset_data(self):
        # self.debug.debug("* "+self.display_name+" -- reset_data",5)
        self.job_wait_size = 0
        self.submit_indices = []
        self.wait_indices = []
        self.run_indices = []
        # self.job_done_list=[]
        self.num_delete_jobs = 0

    # TODO: read file is still a mess

    def _read_header(self, file: Optional[IO[str]] = None):
        """
        Read the header of the job file.
        """
        if file is None:
            file = self.file
        assert file is not None

        pos = file.tell()
        line = file.readline()
        self.swf_loader.load_line(line)

        while line and not self.swf_loader.header_parse_done:
            pos = file.tell()
            line = file.readline()
            self.swf_loader.load_line(line)

        file.seek(pos)

    def _skip_anchor(self, file: Optional[IO[str]] = None):
        """
        Skip the anchor lines of the job file.
        """
        if file is None:
            file = self.file
        assert file is not None

        for _ in range(self.anchor):
            job: Optional[Job] = None

            while job is None:
                line = file.readline()
                if not line:
                    return
                job = self.swf_loader.load_line(line)

    # TODO: why not read multiple lines at a time?

    def _readline(self, file: Optional[IO[str]] = None):
        if file is None:
            file = self.file
        assert file is not None

        line = file.readline()
        if not line:
            # when no more line, readline() will return empty str
            return None

        job: Optional[Job] = None
        while line and job is None:
            line = file.readline()
            job = self.swf_loader.load_line(line)
        if job is None:
            return None

        if self.min_submit_time is None:
            self.min_submit_time = job["submit_time"]
            if self.temp_start < 0:
                self.temp_start = self.min_submit_time
            self.start_offset_B = self.min_submit_time - self.temp_start

        trace = JobTraceInfo(
            id=job["id"],
            submit=self.density * (job["submit_time"] - self.min_submit_time)
            + self.temp_start,
            wait=job["wait_time"],
            run=job["run_time"],
            usedProc=job["allocated_processors"],
            usedAveCPU=job["average_cpu_time"],
            usedMem=job["used_memory"],
            reqProc=job["requested_number_processors"],
            reqTime=job["requested_time"],
            reqMem=job["requested_memory"],
            status=job["status"],
            userID=job["user_id"],
            groupID=job["group_id"],
            num_exe=job["executable_number"],
            num_queue=job["queue_number"],
            num_part=job["partition_number"],
            num_pre=job["previous_job_id"],
            thinkTime=job["think_time_from_previous_job"],
            start=-1,
            end=-1,
            score=0,
            state=0,
            happy=-1,
            estStart=-1,
        )
        self.traces.append(trace)
        self.submit_indices.append(len(self.traces) - 1)

        return trace

    def initial_import_job_file(self, job_file):
        self.temp_start = self.start
        # regex_str = "([^;\\n]*)[;\\n]"
        self.min_submit_time = -1
        self.traces = []
        self.reset_data()
        self.debug.line(4)

        self.file = open(job_file, "r")
        # read header, then skip self.anchor lines
        self.swf_loader = SWFLoader(False)
        self._read_header()
        self._skip_anchor()

    def dyn_import_job_file(self):
        if self.file is None:
            return False
        if self.file.closed:
            return False

        for _ in range(self.read_input_freq):
            if self.read_num is not None and len(self.traces) >= self.read_num:
                self.file.close()
                break
            if not self._readline():
                self.file.close()
                break

        return True

    def import_job_file(self, job_file):
        # self.debug.debug("* "+self.display_name+" -- import_job_file",5)
        self.start
        regex_str = "([^;\\n]*)[;\\n]"
        job_file = open(job_file, "r")
        self.traces = []
        self.reset_data()
        self.swf_loader = SWFLoader(False)
        self._read_header(job_file)
        self._skip_anchor(job_file)

        self.debug.line(4)
        while self.read_num is None or len(self.traces) >= self.read_num:
            if not self._readline(job_file):
                job_file.close()
                break

        self.debug.line(4)
        job_file.close()

    # XREF: JobFilterSWF.output_job_config()
    def import_job_config(self, config_file):
        # self.debug.debug("* "+self.display_name+" -- import_job_config",5)
        regex_str = "([^=\\n]*)[=\\n]"
        jobFile = open(config_file, "r")
        config_data = {}

        self.debug.line(4)
        while 1:
            tempStr = jobFile.readline()
            if not tempStr:  # break when no more line
                break
            temp_dataList = re.findall(regex_str, tempStr)
            config_data[temp_dataList[0]] = temp_dataList[1]
            self.debug.debug(str(temp_dataList[0]) + ": " + str(temp_dataList[1]), 4)
        self.debug.line(4)
        jobFile.close()
        self.start_offset_A = config_data["start_offset"]
        self.start_date = config_data["date"]

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
        return self.job_wait_size

    def refresh_score(self, scores: list[float]):
        # self.debug.debug("* "+self.display_name+" -- refresh_score",5)
        for index, score in zip(self.wait_indices, scores):
            self.traces[index]["score"] = score
        self.wait_indices.sort(key=lambda x: self.traces[x]["score"], reverse=True)

    def job_infos(self):
        return self.traces

    def job_info(self, job_index):
        # self.debug.debug("* "+self.display_name+" -- job_info",6)
        assert job_index >= 0
        return self.traces[job_index]

    def job_info_len(self):
        return len(self.traces) + self.num_delete_jobs

    def job_submit(self, job_index: int, job_score: int = 0, job_est_start: int = -1):
        # self.debug.debug("* "+self.display_name+" -- job_submit",5)
        self.traces[job_index]["state"] = 1
        self.traces[job_index]["score"] = job_score
        self.traces[job_index]["estStart"] = job_est_start
        self.submit_indices.remove(job_index)
        self.wait_indices.append(job_index)
        self.job_wait_size += self.traces[job_index]["reqProc"]
        return 1

    def job_start(self, job_index, time):
        # self.debug.debug("* "+self.display_name+" -- job_start",5)
        self.debug.debug(
            " "
            + "["
            + str(job_index)
            + "]"
            + " Req:"
            + str(self.traces[job_index]["reqProc"])
            + " Run:"
            + str(self.traces[job_index]["run"])
            + " ",
            4,
        )
        self.traces[job_index]["state"] = 2
        self.traces[job_index]["start"] = time
        self.traces[job_index]["wait"] = time - self.traces[job_index]["submit"]
        self.traces[job_index]["end"] = time + self.traces[job_index]["run"]
        self.wait_indices.remove(job_index)
        self.run_indices.append(job_index)
        self.job_wait_size -= self.traces[job_index]["reqProc"]
        return 1

    def job_finish(self, job_index, time=None):
        # self.debug.debug("* "+self.display_name+" -- job_finish",5)
        self.debug.debug(
            " "
            + "["
            + str(job_index)
            + "]"
            + " Req:"
            + str(self.traces[job_index]["reqProc"])
            + " Run:"
            + str(self.traces[job_index]["run"])
            + " ",
            4,
        )
        self.traces[job_index]["state"] = 3
        if time:
            self.traces[job_index]["end"] = time
        self.run_indices.remove(job_index)
        # self.job_done_list.append(job_index)
        return 1

    def job_set_score(self, job_index, job_score):
        self.traces[job_index]["score"] = job_score

    def remove_job_from_dict(self, job_index):
        self.num_delete_jobs += 1
        return self.traces.pop(job_index)
