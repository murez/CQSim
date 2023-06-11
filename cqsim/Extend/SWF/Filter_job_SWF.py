import re
import pandas as pd
from cqsim.CqSim.Job_trace import JobTraceInfo
from cqsim.Filter.Filter_job import Filter_job


class Filter_job_SWF(Filter_job):
    def reset_config_data(self):
        self.config_start = ";"
        self.config_sep = "\\n"
        self.config_equal = ": "
        self.config_data = [
            {"name_config": "date", "name": "StartTime", "value": ""},
            {"name_config": "start_offset", "name": None, "value": ""},
        ]

    def feed_job_trace(self):
        if not self.save:
            print("Save file not set!")
            return
        self.read_job_trace()
        # assert "df" in vars(self).keys()
        input_df = pd.DataFrame(self.jobList)
        # write to file
        # just get the first 18 columns
        save_df = input_df.iloc[:, :18]
    
        save_df.to_csv(self.save, header=False, index=False, sep=";")

    def read_job_trace(self):
        min_sub = -1
        temp_readNum = 0
        # read job trace as csv and ignore ;
        df = pd.read_csv(self.trace, header=None, comment=";", sep="\s+")
        # set column name

        df.columns = list(JobTraceInfo.__annotations__.keys())[:18]
        # other column used by cqsim
        default_extension_info = {
            "start": -1,
            "end": -1,
            "score": 0,
            "state": 0,
            "happy": -1,
            "estStart": -1,
        }
        

        # set extension to dataframe
        for ext_col_key, default_value in default_extension_info.items():
            df[ext_col_key] = default_value
        # set type
        for col_key, type_value in JobTraceInfo.__annotations__.items():
            df[col_key] = df[col_key].astype(type_value)


        # set job list
        index_to_delete = []
        for index, row in df.iterrows():
            tempInfo = JobTraceInfo(row.to_dict())

            if min_sub < 0:
                min_sub = float(tempInfo['submit'])
                if self.start < 0:
                    self.start = min_sub
                for con_data in self.config_data:
                    if (
                        not con_data["name"]
                        and con_data["name_config"] == "start_offset"
                    ):
                        con_data["value"] = min_sub - self.start
                        break

            tempInfo['submit'] = self.density * (tempInfo['submit'] - min_sub) + self.start

            for col_key, type_value in JobTraceInfo.__annotations__.items():
                tempInfo[col_key] = type_value(tempInfo[col_key])

            if self.input_check(tempInfo) >= 0:
                self.jobList.append(tempInfo)
                temp_readNum += 1
            else:
                index_to_delete.append(index)
        # set job num
        self.jobNum = temp_readNum
        # delete invalid job
        df = df.drop(index_to_delete)
        self.df = df
        # read config which is lines begin with ;
        with open(self.trace, "r") as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith(";"):
                    for con_data in self.config_data:
                        if con_data["name"]:
                            con_ex = (
                                con_data["name"]
                                + self.config_equal
                                + "([^"
                                + self.config_sep
                                + "]*)"
                                + self.config_sep
                            )
                            temp_con_List = re.findall(con_ex, line)
                            if len(temp_con_List) >= 1:
                                con_data["value"] = temp_con_List[0]
                                break

    def input_check(self, jobInfo):
        if int(jobInfo["run"]) > int(jobInfo["reqTime"]):
            jobInfo["run"] = jobInfo["reqTime"]
        if int(jobInfo["id"]) <= 0:
            return -2
        if int(jobInfo["submit"]) < 0:
            return -3
        if int(jobInfo["run"]) <= 0:
            return -4
        if int(jobInfo["reqTime"]) <= 0:
            return -5
        if int(jobInfo["reqProc"]) <= 0:
            return -6
        return 1

    def output_job_data(self):
        output_df = pd.DataFrame(self.jobList)
        output_df = output_df.iloc[:, :18]
        output_df.to_csv(self.save, header=False, index=False, sep=";")

    def output_job_config(self):
        if not self.config:
            print("Config file not set!")
            return

        format_equal = "="
        f2 = open(self.config, "w")

        for con_data in self.config_data:
            f2.write(str(con_data["name_config"]))
            f2.write(format_equal)
            f2.write(str(con_data["value"]))
            f2.write("\n")
        f2.close()
