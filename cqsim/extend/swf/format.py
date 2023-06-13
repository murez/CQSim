"""
The Standard Workload Format
"""

from __future__ import annotations

import io
from typing import IO, Callable, Optional, Sequence

import pandas as pd

from cqsim.cqsim.types import Job
from cqsim.types import FilePathOrBuffer
from cqsim.utils import dataclass_types, dataclass_types_for_pandas


class SWF:
    headers: dict[str, str]
    jobs: list[Job]

    def __init__(self, headers: dict[str, str], jobs: list[Job]) -> None:
        self.headers = headers
        self.jobs = jobs


class SWFLoader:
    seperator = ";"
    header_or_comment_prefix = ";"
    header_key_seperator = ":"
    last_header_indent: Optional[int]

    header_parse_done = False
    last_header_key: Optional[str] = None

    remember_jobs: bool
    headers: dict[str, str]
    jobs: list[Job]

    def read_header(self, file: io.TextIOBase):
        """
        Read the header of the job file.
        """
        assert file is not None

        pos = file.tell()
        line = file.readline()
        self.load_line(line)

        while line and not self.header_parse_done:
            pos = file.tell()
            line = file.readline()
            self.load_line(line)

        file.seek(pos)

    def _skip_anchor(self, file: IO[str], anchor: int):
        """
        Skip the anchor lines of the job file.
        """
        assert file is not None

        for _ in range(anchor):
            job: Optional[Job] = None

            while job is None:
                line = file.readline()
                if not line:
                    return
                job = self.load_line(line)

    def __init__(self, remember_jobs: bool = True):
        assert self.header_parse_done == False
        self.headers = {}
        self.jobs = []
        self.remember_jobs = remember_jobs
        self.last_header_indent = None

    def parse_job_line(self, line: str) -> Job:
        fields = line.split()
        if len(fields) != 18:
            raise ValueError(f"Invalid SWF line: {line}")
        job = Job(
            id=int(fields[0]),
            submit_time=float(fields[1]),
            wait_time=float(fields[2]),
            run_time=float(fields[3]),
            allocated_processors=int(fields[4]),
            average_cpu_time=float(fields[5]),
            used_memory=float(fields[6]),
            requested_number_processors=int(fields[7]),
            requested_time=float(fields[8]),
            requested_memory=float(fields[9]),
            status=int(fields[10]),
            user_id=int(fields[11]),
            group_id=int(fields[12]),
            executable_number=int(fields[13]),
            queue_number=int(fields[14]),
            partition_number=int(fields[15]),
            previous_job_id=int(fields[16]),
            think_time_from_previous_job=int(fields[17]),
        )
        return job

    def parse_header(self, line: str) -> tuple[Optional[str], str]:
        # lstrip to keep '\n' at the end
        indent = len(line) - len(line.lstrip())
        line = line.lstrip()
        splited = line.split(self.header_key_seperator, maxsplit=1)

        if len(splited) < 2:
            if self.last_header_key is None:
                # a comment
                self.header_parse_done = True
                return (None, line)

        if len(splited) < 2 or (
            self.last_header_key is not None and splited[0].strip() in ["http", "https"]
        ):
            # a continuation of last header
            return (self.last_header_key, line)

        key, value = splited
        # seperator found, this maybe a header
        key_stripped, value_stripped = key.strip(), value.lstrip()
        self.last_header_key = key_stripped
        self.last_header_indent = indent
        return (key_stripped, value_stripped)

    def load_line(self, line: str):
        if line.startswith(self.header_or_comment_prefix):
            if self.header_parse_done:
                # can only be comment
                return
            # remove the prefix, DO NOT STRIP because the indent is important
            line = line[1:]
            # skip empty line
            if line == "":
                self.last_header_key = None
                return

            key, value = self.parse_header(line)
            if key is not None:
                if key in self.headers:
                    self.headers[key] += value
                else:
                    self.headers[key] = value

        else:  # not start with ;
            self.header_parse_done = True
            line = line.strip()
            # ignore empty lines
            if line == "":
                return
            # this line should be data fields
            job = self.parse_job_line(line)
            if self.remember_jobs:
                self.jobs.append(job)
            return job


def load_header(stream: io.TextIOBase | str):
    """Load a SWF file from a stream."""
    lines: list[str] | io.TextIOBase
    loader = SWFLoader()

    if isinstance(stream, str):
        lines = stream.splitlines()
        for line in lines:
            loader.load_line(line)
            if loader.header_parse_done:
                break
        return loader.headers
    elif isinstance(stream, io.TextIOBase):
        loader.read_header(stream)
        return loader.headers
    else:
        raise TypeError(f"Invalid stream type: {type(stream)}")


def load_jobs_df(
    filepath_or_buffer: FilePathOrBuffer,
    swf: bool,
    skiprows: Sequence[int] | int | Callable[[int], bool] = 0,
    nrows: Optional[int] = None,
):
    """Load a SWF file from file or buffer."""
    field_types = dataclass_types_for_pandas(Job)

    # Dynamically set the header or names argument
    # We must use this way because None does not mean argument not set

    if swf:
        df = pd.read_csv(
            filepath_or_buffer,
            delim_whitespace=True,  # sep=r"\s+",
            comment=";",
            names=list(field_types.keys()),
            header=None,
            dtype=field_types,
            skip_blank_lines=True,
            skipinitialspace=True,
            engine="c",
            skiprows=skiprows,
            nrows=nrows,
        )
    else:
        df = pd.read_csv(
            filepath_or_buffer,
            comment=";",
            dtype=field_types,
            skip_blank_lines=True,
            skipinitialspace=True,
            engine="c",
            skiprows=skiprows,
            nrows=nrows,
        )

    return df


def load_jobs(
    filepath_or_buffer: FilePathOrBuffer,
    swf: bool,
    skiprows: Sequence[int] | int | Callable[[int], bool] = 0,
    nrows: Optional[int] = None,
):
    """Load a SWF file from file or buffer."""
    df = load_jobs_df(filepath_or_buffer, swf, skiprows=skiprows, nrows=nrows)

    # types = dataclass_types(Job)
    # https://stackoverflow.com/questions/62647887/preserving-dtypes-when-extracting-a-row-from-a-pandas-dataframe
    for index, row in df.astype(object).iterrows():
        job = Job(**row.to_dict())
        # XXX: Alternative way to convert types:
        # for field in dataclasses.fields(Job):
        #     setattr(job, field.name, types[field.name](getattr(job, field.name)))
        yield job


def load(stream: io.TextIOBase | str, header_only: bool = False, start_offset: int = 0):
    """Load a SWF file from a stream."""
    lines: list[str] | io.TextIOBase
    if isinstance(stream, str):
        lines = stream.splitlines()
    elif isinstance(stream, io.TextIOBase):
        lines = stream
    else:
        raise TypeError(f"Invalid stream type: {type(stream)}")

    assert header_only

    loader = SWFLoader()
    for line in lines:
        loader.load_line(line)
        if header_only and loader.header_parse_done:
            return SWF(loader.headers, [])
    return SWF(loader.headers, loader.jobs[start_offset:])
