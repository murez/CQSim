from typing import Optional

from cqsim.IOModule.Debug_log import Debug_log
from cqsim.types import Time


class Basic_algorithm:
    scoreList: list[float]

    def __init__(
        self,
        ad_mode: int,  # = 0,
        element: tuple[list[str], list[str]],
        debug: Debug_log,
        para_list: Optional[list[str]] = None,
        ad_para_list: Optional[str] = None,
    ):
        self.myInfo = "Basic Algorithm"
        self.ad_mode = ad_mode
        self.element = element
        self.debug = debug
        self.paralist = para_list
        self.ad_paralist = ad_para_list

        self.debug.line(4, " ")
        self.debug.line(4, "#")
        self.debug.debug("# " + self.myInfo, 1)
        self.debug.line(4, "#")

        self.algStr = ""
        self.scoreList = []
        i = 0
        temp_num = len(self.element[0])
        while i < temp_num:
            self.algStr += self.element[0][i]
            i += 1

    def reset(
        self,
        ad_mode: Optional[int] = None,
        element: Optional[list[str]] = None,
        debug: Optional[Debug_log] = None,
        para_list: Optional[str] = None,
        ad_para_list=None,
    ):
        # self.debug.debug("* "+self.myInfo+" -- reset",5)
        if ad_mode:
            self.ad_mode = ad_mode
        if element:
            self.element = element
        if debug:
            self.debug = debug
        if para_list:
            self.paralist = para_list

        self.algStr = ""
        self.scoreList = []
        i = 0
        temp_num = len(self.element[0])
        while i < temp_num:
            self.algStr += self.element[0][i]
            i += 1

    def get_score(self, wait_job, currentTime: Time, para_list=None):
        # self.debug.debug("* "+self.myInfo+" -- get_score",5)
        self.scoreList = []
        waitNum = len(wait_job)
        if waitNum <= 0:
            return []
        else:
            i = 0
            z = currentTime - wait_job[0]["submit"]
            l = wait_job[0]["reqTime"]
            while i < waitNum:
                temp_w = currentTime - wait_job[i]["submit"]
                if temp_w > z:
                    z = temp_w
                if wait_job[i]["reqTime"] < l:
                    l = wait_job[i]["reqTime"]
                i += 1
            i = 0
            if z == 0:
                z = 1
            while i < waitNum:
                s = float(wait_job[i]["submit"])
                # These variable are used in eval expression, do not delete
                t = float(wait_job[i]["reqTime"])
                n = float(wait_job[i]["reqProc"])
                w = int(currentTime - s)
                # Prevent black auto delete
                if s or t or n or w:
                    pass
                self.scoreList.append(float(eval(self.algStr)))
                i += 1
        # self.debug.debug("  Score:"+str(self.scoreList),4)
        return self.scoreList

    def log_analysis(self):
        # self.debug.debug("* "+self.myInfo+" -- log_analysis",5)
        return 1

    def alg_adapt(self, para_in):
        # self.debug.debug("* "+self.myInfo+" -- alg_adapt",5)
        return 1
