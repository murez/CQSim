from __future__ import annotations

import os
from typing import Literal, Protocol, TypeAlias

from pandas._typing import ReadCsvBuffer


# stable
class HasFileno(Protocol):
    def fileno(self) -> int:
        ...


StrOrBytesPath: TypeAlias = str | bytes | os.PathLike
FileDescriptor: TypeAlias = int  # stable
FileDescriptorLike: TypeAlias = int | HasFileno  # stable
FileDescriptorOrPath: TypeAlias = int | StrOrBytesPath
Time: TypeAlias = float
EventCode: TypeAlias = Literal["S", "E", "Q"]
FilePath = str | os.PathLike[str]
FilePathOrBuffer = FilePath | ReadCsvBuffer[bytes] | ReadCsvBuffer[str]
OpenTextModeUpdating: TypeAlias = Literal[
    "r+",
    "+r",
    "rt+",
    "r+t",
    "+rt",
    "tr+",
    "t+r",
    "+tr",
    "w+",
    "+w",
    "wt+",
    "w+t",
    "+wt",
    "tw+",
    "t+w",
    "+tw",
    "a+",
    "+a",
    "at+",
    "a+t",
    "+at",
    "ta+",
    "t+a",
    "+ta",
    "x+",
    "+x",
    "xt+",
    "x+t",
    "+xt",
    "tx+",
    "t+x",
    "+tx",
]
OpenTextModeWriting: TypeAlias = Literal[
    "w", "wt", "tw", "a", "at", "ta", "x", "xt", "tx"
]
OpenTextModeReading: TypeAlias = Literal[
    "r", "rt", "tr", "U", "rU", "Ur", "rtU", "rUt", "Urt", "trU", "tUr", "Utr"
]
OpenTextMode: TypeAlias = (
    OpenTextModeUpdating | OpenTextModeWriting | OpenTextModeReading
)
