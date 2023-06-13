import bisect
from typing import Any, NamedTuple, Optional

import pandas as pd

from cqsim.cqsim.backfill import Backfill, BackfillPara
from cqsim.cqsim.basic_algorithm import BasicAlgorithm
from cqsim.cqsim.info_collect import InfoCollect
from cqsim.cqsim.job_trace import JobTrace
from cqsim.cqsim.types import (
    Event,
    EventPara,
    EventState,
    EventType,
    NodeInfo,
    WaitInfo,
)
from cqsim.cqsim.window import StartWindow
from cqsim.extend.swf.node import NodeSWF
from cqsim.logging.debug import DebugLog
from cqsim.logging.file import LogFile
from cqsim.logging.output import OutputLog
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
    previous_read_job_time: Optional[Time]

    # An event sequence
    event_seq: list[Event]

    monitor: Optional[int]
    monitor_start: int

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
        self.previous_read_job_time = None  # lastest read job submit time

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
        """The main process of the simulator."""

        # Initialize the event sequence with the job submit event, monitor event and extend event.
        self.import_job_events()
        self.insert_extend_events()

        # Scan the event sequence and deal with all the event in the sequence
        self.scan_event()

        # Output the job result
        self.print_result()
        self.debug.debug("------ Simulating Done!", 2)
        self.debug.debug(lvl=1)

        return

    def import_job_events(self):
        """Read the job trace and insert the job submit event in the event sequence in time order."""

        for i in range(self.module.job.job_info_len()):
            event = Event(
                EventType.JOB,
                self.module.job.job_info(i).submit_time,
                2,
                EventPara(state=EventState.SUBMIT, job_index=i),
            )
            self.insert_event(event)
            self.previous_read_job_time = self.module.job.job_info(i).submit_time
            self.debug.debug(
                "  "
                + "Insert job["
                + "2"
                + "] "
                + str(self.module.job.job_info(i).submit_time),
                4,
            )

    def insert_monitor_events(self, start: Time, end: Time):
        """Insert monitor event from current time to time of the next event."""
        if not self.monitor:
            return

        monitor_time = int(start // self.monitor) * self.monitor

        # self.monitor_start=self.event_pointer
        self.monitor_start = 0

        while monitor_time < end:
            if monitor_time >= start:
                event = Event(EventType.MONITOR, monitor_time, 5, None)
                self.insert_event(event)
                self.debug.debug(
                    "  " + "Insert mon[" + "5" + "] " + str(monitor_time), 4
                )
            monitor_time += self.monitor

    def insert_extend_events(self):
        # self.debug.debug("# "+self.display_name+" -- insert_event_extend",5)
        return

    def insert_event(self, event: Event):
        """Insert the event in the event sequence in time order"""
        index: Optional[int] = None
        if event.type == EventType.JOB:
            index = bisect.bisect_right(self.event_seq, event)
        elif event.type == EventType.MONITOR:
            index = self.get_index_monitor()

        if index is None or index >= len(self.event_seq):
            self.event_seq.append(event)
        else:
            self.event_seq.insert(index, event)

    def delete_event(self, type: Any, time: Time, index: int):
        # self.debug.debug("# "+self.display_name+" -- delete_event",5)
        return

    def get_index_monitor(self):
        # self.debug.debug("# "+self.display_name+" -- get_index_monitor",5)
        self.monitor_start += 1
        return self.monitor_start

    def scan_event(self):
        """
        Scan the event sequence and deal with all the event in the sequence

        `scan_event` will set `self.current_event` to the current event.
        Then call the corresponding `event_[type]` function to process the event.
        """
        self.debug.line(2, " ")

        self.debug.line(2, "=")
        self.debug.line(2, "=")
        self.current_event = None

        while len(self.event_seq) > 0:
            self.current_event = self.event_seq.pop(0)
            self.current_time = self.current_event.time

            if self.current_event.type == EventType.JOB:
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

                self.event_job(self.current_event.para)
            elif self.current_event.type == EventType.MONITOR:
                self.event_monitor(self.current_event.para)
            elif self.current_event.type == EventType.EXTEND:
                self.event_extend(self.current_event.para)
            self.sys_collect(self.current_event)
            self.interface()

        self.debug.line(2, "=")
        self.debug.line(2, "=")
        self.debug.line(2, " ")
        return

    # Event process functions

    def event_job(self, para_in: Optional[EventPara] = None):
        """
        Process the job event.
        """

        current_event = self.current_event
        assert current_event and current_event.para

        # Deal with the submit/finish job event.

        if current_event.para.state == EventState.SUBMIT:
            self.submit(current_event.para.job_index)
        elif current_event.para.state == EventState.FINISH:
            self.finish(current_event.para.job_index)

        # Calculate the scores of the waiting job after the event is done.
        self.score_calculate()

        # Call the start scan method group: window - start new job - backfill
        self.start_scan()

        if self.event_seq:
            # self.insert_event_monitor(self.currentTime, self.event_seq[self.event_pointer+1]['time'])
            self.insert_monitor_events(self.current_time, self.event_seq[0].time)

    def event_monitor(self, para_in: Optional[EventPara] = None):
        # Deal with the monitor event

        # Call the adapt functions.
        self.alg_adapt()
        self.window_adapt()
        # Call the print_adapt() method if needed.
        self.print_adapt(None)

    def event_extend(self, para_in: Optional[EventPara] = None):
        # Deal with the extend event
        # Call the extend process.
        pass

    def submit(self, job_index: int):
        """Submit the job by calling the corresponding method in job_trace module."""
        self.debug.debug("[Submit]  " + str(job_index), 3)
        self.module.job.job_submit(job_index)

    def finish(self, job_index: int):
        """
        Finish the job by calling the corresponding method in job_trace module.
        Also insert job finish event.
        """
        self.debug.debug("[Finish]  " + str(job_index), 3)
        self.module.node.node_release(job_index, self.current_time)
        self.module.job.job_finish(job_index)
        self.module.output.print_result(self.module.job, job_index)
        self.module.job.remove_job_from_dict(job_index)

    def start(self, job_index: int):
        """Start the job by calling the corresponding method in job_trace module."""

        self.debug.debug("[Start]  " + str(job_index), 3)
        self.module.node.node_allocate(
            self.module.job.job_info(job_index).requested_number_processors,
            job_index,
            self.current_time,
            self.current_time + self.module.job.job_info(job_index).requested_time,
        )
        self.module.job.job_start(job_index, self.current_time)
        event = Event(
            EventType.JOB,
            self.current_time + self.module.job.job_info(job_index).run_time,
            1,
            EventPara(state=EventState.FINISH, job_index=job_index),
        )
        self.insert_event(event)

    def score_calculate(self):
        """Calculate the scores of waiting job after the event is done"""
        wait_jobs = [self.module.job.job_info(i) for i in self.module.job.wait_list()]
        score_list = self.module.alg.get_score(wait_jobs, self.current_time)
        self.module.job.refresh_score(score_list)

    def start_scan(self):
        """Call the start scan method group: window - start new job - backfill"""
        start_max = self.module.win.start_num()
        wait_list = self.module.job.wait_list()
        win_count = start_max

        for job_index in wait_list:
            if win_count >= start_max:
                win_count = 0
                wait_list = self.start_window(wait_list)
            # print "....  ", temp_wait[i]
            job = self.module.job.job_info(job_index)
            if self.module.node.is_available(job.requested_number_processors):
                self.start(job_index)
            else:
                wait_list = self.module.job.wait_list()
                self.backfill(wait_list)
                break
            win_count += 1

    def start_window(self, job_indices: list[int]):
        """
        Call the window function to modify the order of the waiting job.

        :return: the new reorder list.
        """
        win_size = self.module.win.window_size()
        targets, keeps = job_indices[:win_size], job_indices[win_size:]

        wait_jobs: list[WaitInfo] = []
        for job_index in targets:
            job = self.module.job.job_info(job_index)
            wait_jobs.append(
                WaitInfo(
                    index=job_index,
                    proc=job.requested_number_processors,
                    node=job.requested_number_processors,
                    run=job.run_time,
                    score=job.score,
                )
            )

        targets = self.module.win.start_window(
            wait_jobs, BackfillPara(time=self.current_time)
        )

        return targets + keeps

    def backfill(self, waiting_jobs: list[int]):
        """
        Call the backfill function and get the backfill job list, then start these jobs.

        :param wait_list: the waiting job list.
        :return: True if any job is started, False otherwise.
        """
        wait_info: list[WaitInfo] = []
        for index in waiting_jobs:
            job = self.module.job.job_info(index)
            wait_info.append(
                WaitInfo(
                    index=index,
                    proc=job.requested_number_processors,
                    node=job.requested_number_processors,
                    run=job.run_time,
                    score=job.score,
                )
            )
        backfill_list = self.module.backfill.backfill(
            wait_info, BackfillPara(time=self.current_time)
        )

        if backfill_list is None:
            return False

        for job in backfill_list:
            self.start(job)
        return True

    def sys_collect(self, current_event: Event):
        """
        Collect the current system information and call the Info_collect module to store them,
        then print the current system information.

        :param current_event: the current event
        """
        temp_inter = 0
        if len(self.event_seq) > 0:
            temp_inter = self.event_seq[0].time - self.current_time

        event_code: Optional[EventCode] = None
        if current_event.type == EventType.JOB:
            assert current_event.para
            if current_event.para.state == EventState.SUBMIT:
                event_code = "S"
            elif current_event.para.state == EventState.FINISH:
                event_code = "E"
        elif current_event.type == EventType.MONITOR:
            event_code = "Q"
        temp_info = self.module.info.info_collect(
            time=self.current_time,
            event=event_code,
            uti=(self.module.node.get_tot() - self.module.node.get_idle())
            * 1.0
            / self.module.node.get_tot(),
            wait_num=len(self.module.job.wait_list()),
            wait_size=self.module.job.wait_size(),
            inter=temp_inter,
        )
        self.print_sys_info(temp_info)

    def interface(self, sys_info: Any = None):
        """Call the running time user interface module to show the inforamtion."""

    def backfill_adapt(self):
        """Call the adapt method in backfill module to modify the parameter of backfill in the monitor event process."""

    def alg_adapt(self):
        """Call the adapt method in Basic_algorithm module to modify the algorithms in the monitor event process."""

    def window_adapt(self):
        """Call the adapt method in Start_window module to modify the parameter of window in the monitor event process."""

    def print_sys_info(self, sys_info: NodeInfo):
        self.module.output.print_sys_info(sys_info)

    def print_adapt(self, adapt_info: Optional[LogFile] = None):
        self.module.output.print_adapt(adapt_info)

    def print_result(self):
        self.module.output.print_sys_info()
        self.debug.debug(lvl=1)
        self.module.output.print_result(self.module.job)
