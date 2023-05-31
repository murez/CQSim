from typing import NamedTuple, Optional

from cqsim.cqsim.backfill import Backfill, BackfillPara
from cqsim.cqsim.basic_algorithm import BasicAlgorithm
from cqsim.cqsim.info_collect import InfoCollect
from cqsim.cqsim.job_trace import JobTrace
from cqsim.cqsim.types import Event, NodeInfo, WaitInfo
from cqsim.cqsim.window import StartWindow
from cqsim.extend.swf.node import NodeSWF
from cqsim.IOModule.debug import DebugLog
from cqsim.IOModule.file import LogFile
from cqsim.IOModule.output import OutputLog
from cqsim.types import EventCode, Time


class ModuleList(NamedTuple):
    job: JobTrace
    node: NodeSWF
    backfill: Backfill
    win: StartWindow
    alg: BasicAlgorithm
    info: InfoCollect
    output: OutputLog


class Cqsim:
    debug: DebugLog
    current_time: Time

    def __init__(
        self,
        module: ModuleList,
        debug: DebugLog,
        monitor: Optional[int] = None,
    ):
        self.display_name = "Cqsim Sim"
        self.module = module
        self.debug = debug
        self.monitor = monitor

        self.debug.line(4, " ")
        self.debug.line(4, "#")
        self.debug.debug("# " + self.display_name, 1)
        self.debug.line(4, "#")

        self.event_seq = []
        # self.event_pointer = 0
        self.monitor_start = 0
        self.current_event = None
        self.current_time = 0
        self.previous_read_job_time = -1  # lastest read job submit time

        self.debug.line(4)
        for m in self.module:
            temp_name = m.display_name
            self.debug.debug(temp_name + " ................... Load", 4)
            self.debug.line(4)

    def reset(
        self,
        module: Optional[ModuleList] = None,
        debug: Optional[DebugLog] = None,
        monitor: Optional[int] = None,
    ):
        # self.debug.debug("# "+self.display_name+" -- reset",5)
        if module:
            self.module = module

        if debug:
            self.debug = debug
        if monitor:
            self.monitor = monitor

        self.event_seq: list[Event] = []
        # self.event_pointer = 0
        self.monitor_start = 0
        self.current_event = None
        self.current_time = 0
        self.previous_read_job_time = -1

    def cqsim_sim(self):
        self.import_submit_events()
        self.insert_event_extend()
        self.scan_event()
        self.print_result()
        self.debug.debug("------ Simulating Done!", 2)
        self.debug.debug(lvl=1)
        return

    def import_submit_events(self):
        # while (i < len(self.module['job'].job_infos())):
        for i in range(self.module.job.job_info_len()):
            self.insert_event(1, self.module.job.job_info(i)["submit"], 2, [1, i])
            self.previous_read_job_time = self.module.job.job_info(i)["submit"]
            self.debug.debug(
                "  "
                + "Insert job["
                + "2"
                + "] "
                + str(self.module.job.job_info(i)["submit"]),
                4,
            )

    def insert_event_monitor(self, start, end):
        # self.debug.debug("# "+self.display_name+" -- insert_event_monitor",5)
        if not self.monitor:
            return -1
        temp_num = start / self.monitor
        temp_num = int(temp_num)
        temp_time = temp_num * self.monitor

        # self.monitor_start=self.event_pointer
        self.monitor_start = 0

        while temp_time < end:
            if temp_time >= start:
                self.insert_event(2, temp_time, 5, None)
                self.debug.debug("  " + "Insert mon[" + "5" + "] " + str(temp_time), 4)
            temp_time += self.monitor
        return

    def insert_event_extend(self):
        # self.debug.debug("# "+self.display_name+" -- insert_event_extend",5)
        return

    def insert_event(
        self, type: int, time: Time, priority: int, para: Optional[list[int]] = None
    ):
        # self.debug.debug("# "+self.display_name+" -- insert_event",5)
        temp_index = -1
        new_event: Event = {"type": type, "time": time, "prio": priority, "para": para}
        if type == 1:
            # i = self.event_pointer
            i = 0
            while i < len(self.event_seq):
                if self.event_seq[i]["time"] == time:
                    if self.event_seq[i]["prio"] > priority:
                        temp_index = i
                        break
                elif self.event_seq[i]["time"] > time:
                    temp_index = i
                    break
                i += 1
        elif type == 2:
            temp_index = self.get_index_monitor()

        if temp_index >= len(self.event_seq) or temp_index == -1:
            self.event_seq.append(new_event)
        else:
            self.event_seq.insert(temp_index, new_event)

    def delete_event(self, type, time, index):
        # self.debug.debug("# "+self.display_name+" -- delete_event",5)
        return

    def get_index_monitor(self):
        # self.debug.debug("# "+self.display_name+" -- get_index_monitor",5)
        """
        if (self.event_pointer>=self.monitor_start):
            self.monitor_start=self.event_pointer+1
        temp_mon = self.monitor_start
        self.monitor_start += 1
        return temp_mon
        """
        self.monitor_start += 1
        return self.monitor_start

    def scan_event(self):
        # self.debug.debug("# "+self.display_name+" -- scan_event",5)
        self.debug.line(2, " ")
        self.debug.line(2, "=")
        self.debug.line(2, "=")
        self.current_event = None
        # while (self.event_pointer < len(self.event_seq) or self.read_job_pointer >= 0):
        while len(self.event_seq) > 0:
            # print('event_seq',len(self.event_seq))
            if len(self.event_seq) > 0:
                temp_current_event = self.event_seq[0]
                temp_current_time = temp_current_event["time"]
            else:
                temp_current_event = None
                temp_current_time = None

            assert temp_current_event and temp_current_time  # TODO
            self.current_event = temp_current_event
            self.current_time = temp_current_time
            if self.current_event["type"] == 1:
                self.debug.line(2, " ")
                self.debug.line(2, ">>>")
                self.debug.line(2, "--")
                # print ("  Time: "+str(self.currentTime))
                self.debug.debug("  Time: " + str(self.current_time), 2)
                self.debug.debug("   " + str(self.current_event), 2)
                self.debug.line(2, "--")
                self.debug.debug("  Wait: " + str(self.module.job.wait_list()), 2)
                self.debug.debug("  Run : " + str(self.module.job.run_list()), 2)
                self.debug.line(2, "--")
                self.debug.debug(
                    "  Tot:"
                    + str(self.module.node.get_tot())
                    + " Idle:"
                    + str(self.module.node.get_idle())
                    + " Avail:"
                    + str(self.module.node.get_avail())
                    + " ",
                    2,
                )
                self.debug.line(2, "--")

                self.event_job(self.current_event["para"])
            elif self.current_event["type"] == 2:
                self.event_monitor(self.current_event["para"])
            elif self.current_event["type"] == 3:
                self.event_extend(self.current_event["para"])
            self.sys_collect()
            self.interface()
            # self.event_pointer += 1
            del self.event_seq[0]
        self.debug.line(2, "=")
        self.debug.line(2, "=")
        self.debug.line(2, " ")
        return

    def event_job(self, para_in=None):
        # self.debug.debug("# "+self.display_name+" -- event_job",5)
        """
        self.debug.line(2,"xxxxx")
        i = 0
        while (i<len(self.event_seq)):
            self.debug.debug(self.event_seq[i],2)
            i += 1

        self.debug.line(2,"xxxxx")
        self.debug.line(2," ")
        self.debug.line(2," ")
        """
        assert self.current_event and self.current_event["para"]
        if self.current_event["para"][0] == 1:
            self.submit(job_index=self.current_event["para"][1])
        elif self.current_event["para"][0] == 2:
            self.finish(self.current_event["para"][1])
        self.score_calculate()
        self.start_scan()
        # if (self.event_pointer < len(self.event_seq)-1):
        if len(self.event_seq) > 1:
            # self.insert_event_monitor(self.currentTime, self.event_seq[self.event_pointer+1]['time'])
            self.insert_event_monitor(self.current_time, self.event_seq[1]["time"])
        return

    def event_monitor(self, para_in=None):
        # self.debug.debug("# "+self.display_name+" -- event_monitor",5)
        self.alg_adapt()
        self.window_adapt()
        self.print_adapt(None)
        return

    def event_extend(self, para_in=None):
        # self.debug.debug("# "+self.display_name+" -- event_extend",5)
        return

    def submit(self, job_index: int):
        # self.debug.debug("# "+self.display_name+" -- submit",5)
        self.debug.debug("[Submit]  " + str(job_index), 3)
        self.module.job.job_submit(job_index)
        return

    def finish(self, job_index):
        # self.debug.debug("# "+self.display_name+" -- finish",5)
        self.debug.debug("[Finish]  " + str(job_index), 3)
        self.module.node.node_release(job_index, self.current_time)
        self.module.job.job_finish(job_index)
        self.module.output.print_result(self.module.job, job_index)
        self.module.job.remove_job_from_dict(job_index)
        return

    def start(self, job_index):
        # self.debug.debug("# "+self.display_name+" -- start",5)
        self.debug.debug("[Start]  " + str(job_index), 3)
        self.module.job.job_info(job_index)["reqProc"]
        self.module.node.node_allocate(
            self.module.job.job_info(job_index)["reqProc"],
            job_index,
            self.current_time,
            self.current_time + self.module.job.job_info(job_index)["reqTime"],
        )
        self.module.job.job_start(job_index, self.current_time)
        self.insert_event(
            1,
            self.current_time + self.module.job.job_info(job_index)["run"],
            1,
            [2, job_index],
        )
        return

    def score_calculate(self):
        wait_job = [self.module.job.job_info(i) for i in self.module.job.wait_list()]
        score_list = self.module.alg.get_score(wait_job, self.current_time)
        self.module.job.refresh_score(score_list)
        return

    def start_scan(self):
        # self.debug.debug("# "+self.display_name+" -- start_scan",5)
        start_max = self.module.win.start_num()
        temp_wait = self.module.job.wait_list()
        wait_num = len(temp_wait)
        win_count = start_max

        i = 0
        while i < wait_num:
            if win_count >= start_max:
                win_count = 0
                temp_wait = self.start_window(temp_wait)
            # print "....  ", temp_wait[i]
            temp_job = self.module.job.job_info(temp_wait[i])
            if self.module.node.is_available(temp_job["reqProc"]):
                self.start(temp_wait[i])
            else:
                temp_wait = self.module.job.wait_list()
                self.backfill(temp_wait)
                break
            i += 1
            win_count += 1
        return

    def start_window(self, temp_wait_B):
        # self.debug.debug("# "+self.display_name+" -- start_window",5)
        win_size = self.module.win.window_size()

        if len(temp_wait_B) > win_size:
            temp_wait_A = temp_wait_B[0:win_size]
            temp_wait_B = temp_wait_B[win_size:]
        else:
            temp_wait_A = temp_wait_B
            temp_wait_B = []

        temp_wait_info: list[WaitInfo] = []
        max_num = len(temp_wait_A)
        i = 0
        while i < max_num:
            temp_job = self.module.job.job_info(temp_wait_A[i])
            temp_wait_info.append(
                WaitInfo(
                    index=temp_wait_A[i],
                    proc=temp_job["reqProc"],
                    node=temp_job["reqProc"],
                    run=temp_job["run"],
                    score=temp_job["score"],
                )
            )
            i += 1

        temp_wait_A = self.module.win.start_window(
            temp_wait_info, BackfillPara(time=self.current_time)
        )
        temp_wait_B[0:0] = temp_wait_A
        return temp_wait_B

    def backfill(self, temp_wait):
        # self.debug.debug("# "+self.display_name+" -- backfill",5)
        temp_wait_info: list[WaitInfo] = []
        max_num = len(temp_wait)
        i = 0
        while i < max_num:
            temp_job = self.module.job.job_info(temp_wait[i])
            temp_wait_info.append(
                WaitInfo(
                    index=temp_wait[i],
                    proc=temp_job["reqProc"],
                    node=temp_job["reqProc"],
                    run=temp_job["run"],
                    score=temp_job["score"],
                )
            )
            i += 1
        backfill_list = self.module.backfill.backfill(
            temp_wait_info, BackfillPara(time=self.current_time)
        )
        # self.debug.debug("HHHHHHHHHHHHH "+str(backfill_list)+" -- backfill",2)
        if not backfill_list:
            return 0

        for job in backfill_list:
            self.start(job)
        return 1

    def sys_collect(self):
        # self.debug.debug("# "+self.display_name+" -- sys_collect",5)
        """
        temp_inter = 0
        if (self.event_pointer+1<len(self.event_seq)):
            temp_inter = self.event_seq[self.event_pointer+1]['time'] - self.currentTime
        temp_size = 0

        event_code=None
        if (self.event_seq[self.event_pointer]['type'] == 1):
            if (self.event_seq[self.event_pointer]['para'][0] == 1):
                event_code='S'
            elif(self.event_seq[self.event_pointer]['para'][0] == 2):
                event_code='E'
        elif (self.event_seq[self.event_pointer]['type'] == 2):
            event_code='Q'
        """
        temp_inter = 0
        if len(self.event_seq) > 1:
            temp_inter = self.event_seq[1]["time"] - self.current_time

        event_code: Optional[EventCode] = None
        if self.event_seq[0]["type"] == 1:
            assert self.event_seq[0]["para"]
            if self.event_seq[0]["para"][0] == 1:
                event_code = "S"
            elif self.event_seq[0]["para"][0] == 2:
                event_code = "E"
        elif self.event_seq[0]["type"] == 2:
            event_code = "Q"
        temp_info = self.module.info.info_collect(
            time=self.current_time,
            event=event_code,
            uti=(self.module.node.get_tot() - self.module.node.get_idle())
            * 1.0
            / self.module.node.get_tot(),
            waitNum=len(self.module.job.wait_list()),
            waitSize=self.module.job.wait_size(),
            inter=temp_inter,
        )
        self.print_sys_info(temp_info)
        return

    def interface(self, sys_info=None):
        # self.debug.debug("# "+self.display_name+" -- interface",5)
        return

    def alg_adapt(self):
        # self.debug.debug("# "+self.display_name+" -- alg_adapt",5)
        return 0

    def window_adapt(self):
        # self.debug.debug("# "+self.display_name+" -- window_adapt",5)
        return 0

    def print_sys_info(self, sys_info: NodeInfo):
        # self.debug.debug("# "+self.display_name+" -- print_sys_info",5)
        self.module.output.print_sys_info(sys_info)

    def print_adapt(self, adapt_info: Optional[LogFile] = None):
        # self.debug.debug("# "+self.display_name+" -- print_adapt",5)
        self.module.output.print_adapt(adapt_info)

    def print_result(self):
        # self.debug.debug("# "+self.display_name+" -- print_result",5)
        self.module.output.print_sys_info()
        self.debug.debug(lvl=1)
        self.module.output.print_result(self.module.job)
