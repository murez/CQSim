from typing import Optional, TypedDict

from src.CqSim.Info_collect import NodeInfo
from src.CqSim.Job_trace import Job_trace, JobTraceInfo
from src.IOModule.Log_print import Log_print


class Output(TypedDict):
    sys: str
    adapt: str
    result: str


class Output_log:
    job_buf: list[JobTraceInfo]
    sys_info_buf: list[NodeInfo]

    def __init__(self, output: Optional[Output] = None, log_freq=1):
        self.myInfo = "Output_log"
        self.output_path = output
        self.sys_info_buf = []
        self.job_buf = []
        self.log_freq = log_freq
        # print('log_freq+++++++',self.log_freq)
        self.reset_output()

    def reset(self, output: Optional[Output] = None, log_freq=1):
        if output:
            self.output_path = output
            self.sys_info_buf = []
            self.job_buf = []
            self.log_freq = log_freq
            self.reset_output()

    def reset_output(self):
        if self.output_path is None:
            return
        self.sys_info = Log_print(self.output_path["sys"], "w")
        self.sys_info.reset(self.output_path["sys"], "w")
        self.sys_info.file_open()
        self.sys_info.file_close()
        self.sys_info.reset(self.output_path["sys"], "a")

        self.adapt_info = Log_print(self.output_path["adapt"], "w")
        self.adapt_info.reset(self.output_path["adapt"], "w")
        self.adapt_info.file_open()
        self.adapt_info.file_close()
        self.adapt_info.reset(self.output_path["adapt"], "a")

        self.job_result = Log_print(self.output_path["result"], "w")
        self.job_result.reset(self.output_path["result"], "w")
        self.job_result.file_open()
        self.job_result.file_close()
        self.job_result.reset(self.output_path["result"], "a")

    def print_sys_info(self, sys_info=None):
        """
        sep_sign=";"
        sep_sign_B=" "
        #context = "printing ............\n"
        context = ""
        context += str(sys_info['date'])
        context += sep_sign
        context += str(sys_info['event'])
        context += sep_sign
        context += str(sys_info['time'])
        context += sep_sign

        context += ('uti'+'='+str(sys_info['uti']))
        context += sep_sign_B
        context += ('waitNum'+'='+str(sys_info['waitNum']))
        context += sep_sign_B
        context += ('waitSize'+'='+str(sys_info['waitSize']))
        self.sys_info.file_open()
        self.sys_info.log_print(context,1)
        self.sys_info.file_close()
        """
        if sys_info != None:
            self.sys_info_buf.append(sys_info)
        if (len(self.sys_info_buf) >= self.log_freq) or (sys_info == None):
            sep_sign = ";"
            sep_sign_B = " "
            # pre_context = "Printing..............................\n"
            self.sys_info.file_open()
            for sys_info in self.sys_info_buf:
                # context = pre_context+""
                # pre_context = ""
                context = ""
                context += str(int(sys_info["date"]))
                context += sep_sign
                context += str(sys_info["event"])
                context += sep_sign
                context += str(sys_info["time"])
                context += sep_sign

                context += "uti" + "=" + str(sys_info["uti"])
                context += sep_sign_B
                context += "waitNum" + "=" + str(sys_info["waitNum"])
                context += sep_sign_B
                context += "waitSize" + "=" + str(sys_info["waitSize"])
                self.sys_info.log_print(context, 1)
            self.sys_info.file_close()
            self.sys_info_buf = []

    def print_adapt(self, adapt_info: Optional[Log_print] = None):
        sep_sign = ";"
        context = ""
        if adapt_info is None:
            adapt_info = self.adapt_info
        adapt_info.file_open()
        adapt_info.log_print(context, 1)
        adapt_info.file_close()

    def print_result(self, job_module: Job_trace, job_index: Optional[int] = None):
        if job_index != None:
            self.job_buf.append(job_module.job_info(job_index))
        if (len(self.job_buf) >= self.log_freq) or (job_index == None):
            self.job_result.file_open()
            sep_sign = ";"
            for temp_job in self.job_buf:
                # temp_job = job_module.job_info(job_index)
                context = ""
                context += str(temp_job["id"])
                context += sep_sign
                context += str(temp_job["reqProc"])
                context += sep_sign
                context += str(temp_job["reqProc"])
                context += sep_sign
                context += str(temp_job["reqTime"])
                context += sep_sign
                context += str(temp_job["run"])
                context += sep_sign
                context += str(temp_job["wait"])
                context += sep_sign
                context += str(temp_job["submit"])
                context += sep_sign
                context += str(temp_job["start"])
                context += sep_sign
                context += str(temp_job["end"])
                self.job_result.log_print(context, 1)
            self.job_result.file_close()
            self.job_buf = []

    """
    def print_result(self, job_module):
        sep_sign=";"
        context = ""
        self.job_result.file_open()
        i = 0
        done_list = job_module.done_list()
        job_num = len(done_list)
        while (i<job_num):
            temp_job = job_module.job_info(i)
            context = ""
            context += str(temp_job['id'])
            context += sep_sign
            context += str(temp_job['reqProc'])
            context += sep_sign
            context += str(temp_job['reqProc'])
            context += sep_sign
            context += str(temp_job['reqTime'])
            context += sep_sign
            context += str(temp_job['run'])
            context += sep_sign
            context += str(temp_job['wait'])
            context += sep_sign
            context += str(temp_job['submit'])
            context += sep_sign
            context += str(temp_job['start'])
            context += sep_sign
            context += str(temp_job['end'])
            self.job_result.log_print(context,1)

            i += 1
        self.job_result.file_close()
        """
