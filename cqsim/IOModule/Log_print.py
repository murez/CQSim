import os
from typing import IO, Optional

from cqsim.types import OpenTextMode, StrOrBytesPath


class Log_print:
    def __init__(self, filePath: Optional[StrOrBytesPath], mode: OpenTextMode = "w"):
        self.filePath = filePath
        self.mode = mode
        self.logFile: Optional[IO] = None

    def reset(
        self,
        filePath: Optional[StrOrBytesPath] = None,
        mode: Optional[OpenTextMode] = None,
    ):
        if filePath:
            self.filePath = filePath
        if mode:
            self.mode = mode
        self.logFile = None

    def file_open(self):
        if not self.filePath:
            return 0
        # Create directory if not exists
        dirPath = os.path.dirname(str(self.filePath))
        if not os.path.exists(dirPath):
            os.makedirs(dirPath, exist_ok=True)
        self.logFile = open(self.filePath, self.mode)
        return 1

    def file_close(self):
        if not self.logFile:
            return 0
        self.logFile.close()
        return 1

    def log_print(self, context: object, isEnter=1):
        if not self.logFile:
            return
        self.logFile.write(str(context))
        if isEnter == 1:
            self.logFile.write("\n")
