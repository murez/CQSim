from dataclasses import dataclass, field
from typing import Optional

from cqsim.logging.debug import DebugLog


@dataclass
class NodeData:
    id: int
    location: list[int] = field(default_factory=lambda: [1])
    group: int = 1
    state: int = -1
    proc: int = 1
    start: int = -1
    end: int = -1
    extend: Optional[dict[str, str]] = None

    def to_legacy_str(self):
        result = ""
        sep_sign = ";"
        result += str(self.id)
        result += sep_sign
        result += str(self.location)
        result += sep_sign
        result += str(self.group)
        result += sep_sign
        result += str(self.state)
        result += sep_sign
        result += str(self.proc)
        result += "\n"
        return result


@dataclass
class ConfigData:
    max_nodes: int
    max_procs: int


class NodeFilter:
    node_list: list[NodeData]
    config_data: Optional[ConfigData]

    def __init__(self, struc: str, config: str, save: str, debug: DebugLog):
        self.display_name = "Filter Node"
        self.struc = str(struc)
        self.save = str(save)
        self.config = str(config)
        self.debug = debug
        self.node_list = []

        self.debug.line(4, " ")
        self.debug.line(4, "#")
        self.debug.debug("# " + self.display_name, 1)
        self.debug.line(4, "#")

        self.reset_config()

    def reset(
        self,
        struc: Optional[str] = None,
        config: Optional[str] = None,
        save: Optional[str] = None,
        debug: Optional[DebugLog] = None,
    ):
        self.debug.debug("* " + self.display_name + " -- reset", 5)
        if struc:
            self.struc = str(struc)
        if save:
            self.save = str(save)
        if config:
            self.config = str(config)
        if debug:
            self.debug = debug
        self.node_list = []

        self.reset_config()

    def reset_config(self):
        self.debug.debug("* " + self.display_name + " -- reset_config_data", 5)
        self.config_data = None
        # self.config_data.append({'name_config':'date','name':'StartTime','value':''})

    def read_node_structure(self):
        raise NotImplementedError

    def get_node_data(self):
        self.debug.debug("* " + self.display_name + " -- get_node_data", 5)
        return self.node_list

    def dump_node_list(self):
        self.debug.debug("* " + self.display_name + " -- output_node_data", 5)
        if not self.save:
            print("Save file not set!")
            return
        raise NotImplementedError

    def dump_config(self):
        self.debug.debug("* " + self.display_name + " -- output_node_config", 5)
        if not self.config:
            print("Config file not set!")
            return
        raise NotImplementedError
