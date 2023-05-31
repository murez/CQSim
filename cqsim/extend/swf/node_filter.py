import dataclasses
import json

import pandas as pd

from cqsim.extend import swf
from cqsim.filter import NodeFilter
from cqsim.filter.node import ConfigData, NodeData


class NodeFilterSWF(NodeFilter):
    def reset_config(self):
        self.config_data = None

    def read_node_structure(self):
        with open(self.struc, "r") as f:
            structure = swf.load(f)
        self.build_node_list(structure.headers)
        self.nodeNum = len(self.node_list)

        max_procs = int(structure.headers["MaxProcs"])
        max_nodes = int(structure.headers["MaxNodes"])
        self.config_data = ConfigData(max_nodes=max_nodes, max_procs=max_procs)

    def build_node_list(self, node_info: dict[str, str]):
        self.node_list = [NodeData(id=i + 1) for i in range(int(node_info["MaxProcs"]))]

    def dump_node_list(self):
        if not self.save:
            print("Save file not set!")
            return

        # with open(self.save, "w") as f:
        # # https://stackoverflow.com/a/31517812/5509659
        # nodes = [dataclasses.asdict(n) for n in self.node_list]
        # json.dump(nodes, f)

        df = pd.DataFrame([dataclasses.asdict(n) for n in self.node_list])
        df.to_csv(self.save, index=False)

    def dump_config(self):
        if not self.config:
            print("Config file not set!")
            return
        if not self.config_data:
            print("Config data not set!")
            return

        with open(self.config, "w") as f:
            json.dump(dataclasses.asdict(self.config_data), f)
