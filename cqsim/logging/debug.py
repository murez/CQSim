from typing import Optional

from cqsim.logging.file import LogFile
from cqsim.types import StrOrBytesPath


class DebugLog:
    debugFile: LogFile
    DebugLog_buf: list[object]

    def __init__(
        self,
        lvl: int = 2,
        show: int = 2,
        path: Optional[StrOrBytesPath] = None,
        log_freq: int = 1,
    ):
        self.display_name = "Debug"
        self.lvl = lvl
        self.path = path
        self.show = show
        self.debugFile = LogFile(self.path, "w")
        self.DebugLog_buf = []
        self.log_freq = log_freq
        self.reset_log()

    def reset(
        self,
        lvl: Optional[int] = None,
        path: Optional[StrOrBytesPath] = None,
        log_freq: int = 1,
    ):
        if lvl:
            self.lvl = lvl
        if path:
            self.path = path
        self.debugFile.reset(self.path, "w")
        self.DebugLog_buf = []
        self.log_freq = log_freq
        self.reset_log()

    def reset_log(self):
        """
        Clear the log file.
        """
        self.debugFile.reset(self.path, "w")
        self.debugFile.file_open()
        self.debugFile.file_close()
        self.debugFile.reset(self.path, "a")

    def set_lvl(self, lvl: int = 0):
        self.lvl = lvl

    def debug(self, context: Optional[object] = None, lvl: int = 3):
        if lvl <= self.lvl:
            if context != None:
                self.DebugLog_buf.append(context)
            if (len(self.DebugLog_buf) >= self.log_freq) or (context == None):
                self.debugFile.file_open()
                # print self.DebugLog_buf
                for DebugLog in self.DebugLog_buf:
                    self.debugFile.log_print(DebugLog, 1)
                    # print DebugLog
                self.debugFile.file_close()
                self.DebugLog_buf = []
            if (lvl >= self.show) and (context != None):
                print(context)
                # pass

    def line(self, lvl: int = 1, signal: str = "-", num: int = 15):
        if lvl <= self.lvl:
            i = 0
            context = ""
            while i < num:
                context += signal
                i += 1
            self.DebugLog_buf.append(context)
            if len(self.DebugLog_buf) >= self.log_freq:
                self.debugFile.file_open()
                for DebugLog in self.DebugLog_buf:
                    self.debugFile.log_print(DebugLog, 1)
                self.debugFile.file_close()
                self.DebugLog_buf = []
            if lvl >= self.show:
                print(context)
