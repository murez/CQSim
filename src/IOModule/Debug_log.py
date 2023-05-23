from typing import Optional

from IOModule.Log_print import Log_print
from src.types import StrOrBytesPath


class Debug_log:
    debugFile: Log_print
    debug_log_buf: list[object]

    def __init__(
        self, lvl=2, show=2, path: Optional[StrOrBytesPath] = None, log_freq=1
    ):
        self.myInfo = "Debug"
        self.lvl = lvl
        self.path = path
        self.show = show
        self.debugFile = Log_print(self.path, "w")
        self.debug_log_buf = []
        self.log_freq = log_freq
        self.reset_log()

    def reset(
        self,
        lvl: Optional[int] = None,
        path: Optional[StrOrBytesPath] = None,
        log_freq=1,
    ):
        if lvl:
            self.lvl = lvl
        if path:
            self.path = path
        self.debugFile.reset(self.path, "w")
        self.debug_log_buf = []
        self.log_freq = log_freq
        self.reset_log()

    def reset_log(self):
        self.debugFile.reset(self.path, "w")
        self.debugFile.file_open()
        self.debugFile.file_close()
        self.debugFile.reset(self.path, "a")
        return 1

    def set_lvl(self, lvl=0):
        self.lvl = lvl

    def debug(self, context: Optional[object] = None, lvl=3):
        if lvl <= self.lvl:
            if context != None:
                self.debug_log_buf.append(context)
            if (len(self.debug_log_buf) >= self.log_freq) or (context == None):
                self.debugFile.file_open()
                # print self.debug_log_buf
                for debug_log in self.debug_log_buf:
                    self.debugFile.log_print(debug_log, 1)
                    # print debug_log
                self.debugFile.file_close()
                self.debug_log_buf = []
            if (lvl >= self.show) and (context != None):
                print(context)
                # pass

    def line(self, lvl=1, signal="-", num=15):
        if lvl <= self.lvl:
            i = 0
            context = ""
            while i < num:
                context += signal
                i += 1
            self.debug_log_buf.append(context)
            if len(self.debug_log_buf) >= self.log_freq:
                self.debugFile.file_open()
                for debug_log in self.debug_log_buf:
                    self.debugFile.log_print(debug_log, 1)
                self.debugFile.file_close()
                self.debug_log_buf = []
            if lvl >= self.show:
                print(context)
        """
        if (lvl<=self.lvl):
            self.debugFile.file_open()
            i = 0
            context = ""
            while (i<num):
                context += signal
                i += 1
            self.debugFile.log_print(context,1)
            if (lvl>=self.show):
                print context
            self.debugFile.file_close()
        """

    """
    def start_debug(self):
        self.debugFile.file_open()

    def end_debug(self):
        self.debugFile.file_close()
    """
