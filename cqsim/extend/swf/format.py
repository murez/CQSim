"""
The Standard Workload Format
"""

from __future__ import annotations

import io
from typing import IO, Optional

from cqsim.cqsim.types import Job


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

    def _read_header(self, file: IO[str]):
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
            self.last_header_indent is not None and indent > self.last_header_indent
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


def load(stream: io.TextIOBase | str, header_only: bool = False, start_offset: int = 0):
    """Load a SWF file from a stream."""
    lines: list[str] | io.TextIOBase
    if isinstance(stream, str):
        lines = stream.splitlines()
    elif isinstance(stream, io.TextIOBase):
        lines = stream
    else:
        raise TypeError(f"Invalid stream type: {type(stream)}")

    loader = SWFLoader()
    for line in lines:
        loader.load_line(line)
        if header_only and loader.header_parse_done:
            return SWF(loader.headers, [])
    return SWF(loader.headers, loader.jobs[start_offset:])
