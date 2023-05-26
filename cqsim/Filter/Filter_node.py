from typing import Optional

from cqsim.IOModule.Debug_log import Debug_log


class Filter_node:
    def __init__(self, struc: str, config: str, save: str, debug: Debug_log):
        self.myInfo = "Filter Node"
        self.struc = str(struc)
        self.save = str(save)
        self.config = str(config)
        self.debug = debug
        self.nodeNum = -1
        self.nodeList = []

        self.debug.line(4, " ")
        self.debug.line(4, "#")
        self.debug.debug("# " + self.myInfo, 1)
        self.debug.line(4, "#")

        self.reset_config_data()

    def reset(
        self,
        struc: Optional[str] = None,
        config: Optional[str] = None,
        save: Optional[str] = None,
        debug: Optional[Debug_log] = None,
    ):
        self.debug.debug("* " + self.myInfo + " -- reset", 5)
        if struc:
            self.struc = str(struc)
        if save:
            self.save = str(save)
        if config:
            self.config = str(config)
        if debug:
            self.debug = debug
        self.nodeNum = -1
        self.nodeList = []

        self.reset_config_data()

    def reset_config_data(self):
        self.debug.debug("* " + self.myInfo + " -- reset_config_data", 5)
        self.config_start = ";"
        self.config_sep = "\\n"
        self.config_equal = ": "
        self.config_data = []
        # self.config_data.append({'name_config':'date','name':'StartTime','value':''})

    def read_node_struc(self):
        self.debug.debug("* " + self.myInfo + " -- read_node_struc", 5)
        return

    def input_check(self, nodeInfo):
        self.debug.debug("* " + self.myInfo + " -- input_check", 5)
        return

    def get_node_num(self):
        self.debug.debug("* " + self.myInfo + " -- get_node_num", 6)
        return self.nodeNum

    def get_node_data(self):
        self.debug.debug("* " + self.myInfo + " -- get_node_data", 5)
        return self.nodeList

    def output_node_data(self):
        self.debug.debug("* " + self.myInfo + " -- output_node_data", 5)
        if not self.save:
            print("Save file not set!")
            return
        return

    def output_node_config(self):
        self.debug.debug("* " + self.myInfo + " -- output_node_config", 5)
        if not self.config:
            print("Config file not set!")
            return
        return
