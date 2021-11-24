import os
import threading
import pandas as pd
import time
import re
from threading import Thread
from numpy import nan
from pandas import ExcelWriter, read_excel, isnull, DataFrame, Series, isna
from openpyxl.styles import Alignment, numbers

import warnings

warnings.filterwarnings("ignore")
Frequency = "Quarterly"
error_str = ["", "None", 0, "0", " 0", " None"]

# 计算类 通过修改主函数里的countrie一个一个国家的跑 减少意外的发生
# 在QC里标题的基础上再加上一些我们需要的标题，并按照规定的计算方式把每个标题的熟知计算出来
# 将QC里同类型的name合并到一起
# Output: ./Calculate/"Country"/Calculate_"xx".xlsx
class CalculateData(Thread):
    def __init__(self, country, file, path):
        super(CalculateData, self).__init__()
        self.file = file
        self.symbol = re.sub(r"(QCReport_)|.xlsx", "", file)
        self.name = self.symbol
        self.country = country
        self.excel_file = file
        self.excel_path = excel_path
        self.save_path = "Calculate/" + country + "/Calculate_" + self.symbol + ".xlsx"
        self.labels = ["OwnershipData", "ConcentratedOwners", "Buyers", "Sellers"]
        self.labels_1 = ["OwnershipData", "ConcentratedOwners", "Buyers", "Sellers"]
        self.funds_df = read_excel(path + file, sheet_name="Funds")
        self.institutions_df = read_excel(path + file, sheet_name="Institutions")
        self.writer = ExcelWriter(self.save_path, engine="openpyxl")

    def reformat_excel(self):
        self.excel_file = self.excel_file.replace('QCReport','Download')
        x1 = pd.ExcelFile(self.excel_path + self.excel_file)
        data = x1.sheet_names
        if 'Funds_OwnershipData' not in data:
            self.labels.remove('OwnershipData')
        if 'Funds_ConcentratedOwners' not in data:
            self.labels.remove('ConcentratedOwners')
        if 'Funds_Buyers' not in data:
            self.labels.remove('Buyers')
        if 'Funds_Sellers' not in data:
            self.labels.remove('Sellers')

        for n, label in enumerate(self.labels):
            self.funds_df.insert(loc=4 + 13 * n, column=label + "_Annualized", value=nan)
            self.funds_df.insert(loc=5 + 13 * n, column=label + "_Annualized_YoY", value=nan)
            self.funds_df.insert(loc=6 + 13 * n, column=label + "_Annualized_QoQ", value=nan)
            self.funds_df.insert(loc=7 + 13 * n, column=label + "_YoY", value=nan)
            self.funds_df.insert(loc=8 + 13 * n, column=label + "_QoQ", value=nan)
            self.funds_df.insert(loc=9 + 13 * n, column=label + "_Fund/Institutions_TotalShares", value=nan)
            self.funds_df.insert(loc=10 + 13 * n, column=label + "_Fund/Institutions_TotalAssets", value=nan)

        if 'Institutions_OwnershipData' not in data:
            self.labels_1.remove('OwnershipData')
        if 'Institutions_ConcentratedOwners' not in data:
            self.labels_1.remove('ConcentratedOwners')
        if 'Institutions_Buyers' not in data:
            self.labels_1.remove('Buyers')
        if 'Institutions_Sellers' not in data:
            self.labels_1.remove('Sellers')

        for n, label in enumerate(self.labels_1):
            self.institutions_df.insert(loc=4 + 11 * n, column=label + "_Annualized", value=nan)
            self.institutions_df.insert(loc=5 + 11 * n, column=label + "_Annualized_YoY", value=nan)
            self.institutions_df.insert(loc=6 + 11 * n, column=label + "_Annualized_QoQ", value=nan)
            self.institutions_df.insert(loc=7 + 11 * n, column=label + "_YoY", value=nan)
            self.institutions_df.insert(loc=8 + 11 * n, column=label + "_QoQ", value=nan)
            self.institutions_df.insert(loc=9 + 11 * n, column=label + "_Fund/Institutions_TotalShares", value=nan)
            self.institutions_df.insert(loc=10 + 11 * n, column=label + "_Fund/Institutions_TotalAssets", value=nan)

        self.funds_df.insert(loc=4, column="Source", value="Funds")
        self.institutions_df.insert(loc=4, column="Source", value="Institutions")
    # 对所需要的head进行计算
    def calculate(self):
        funds_save_df = DataFrame(columns=self.funds_df.columns)
        names = list(set(self.funds_df.loc[:, "Name"]))
        names.sort(key=self.funds_df.loc[:, "Name"].to_list().index)
        if nan in names:
            names.remove(nan)
        if "Total (for Top 20)" in names:
            names.remove("Total (for Top 20)")
        for name in names:
            funds_df = self.funds_df.set_index("Name").loc[name, :]
            if type(funds_df) == Series:
                funds_df = funds_df.rename_axis()
                funds_df["Name"] = funds_df.name
                funds_df = DataFrame().append(funds_df)
            funds_df = funds_df.reset_index()
            funds_df = self.format_value(funds_df)

            funds_save_df = funds_save_df.append(funds_df)

        institutions_save_df = DataFrame(columns=self.institutions_df.columns)
        names = list(set(self.institutions_df.loc[:, "Name"].to_list()))
        names.sort(key=self.institutions_df.loc[:, "Name"].to_list().index)
        if nan in names:
            names.remove(nan)
        if "Total (for Top 20)" in names:
            names.remove("Total (for Top 20)")
        for name in names:
            institutions_df = self.institutions_df.set_index("Name").loc[name, :]
            if type(institutions_df) == Series:
                institutions_df = institutions_df.rename_axis()
                institutions_df["Name"] = institutions_df.name
                institutions_df = DataFrame().append(institutions_df)
            institutions_df = institutions_df.reset_index()
            institutions_df = self.format_value(institutions_df)
            institutions_save_df = institutions_save_df.append(institutions_df)

        funds_save_df.to_excel(self.writer, sheet_name="Funds", index=False)
        institutions_save_df.to_excel(self.writer, sheet_name="Institutions", index=False)

        wb = self.writer.book
        for source in ["Funds", "Institutions"]:
            for cell in wb[source][1]:
                cell.alignment = Alignment(wrap_text=True)
            if self.country == "Shanghai_Shenzhen":
                for cell in wb[source]["A"]:
                    cell.value = str(cell.value)
                    cell.value.zfill(6)
                    cell.number_format = numbers.FORMAT_TEXT
            wb[source].freeze_panes = "F2"
        self.writer.save()
        self.writer.close()
    # 对数值进行规范统一处理
    def format_value(self, data_df):
        for label in self.labels:
            share_label = label + "_CurrentShares"
            annualized_label = label + "_Annualized"
            annualized_yoy_label = label + "_Annualized_YoY"
            yoy_label = label + "_YoY"
            annualized_qoq_label = label + "_Annualized_QoQ"
            qoq_label = label + "_QoQ"
            shares = label + "_TotalSharesHeld"
            assets = label + "_TotalAssets"
            total_shares = label + "_Fund/Institutions_TotalShares"
            total_assets = label + "_Fund/Institutions_TotalAssets"

            for m in range(data_df.shape[0]):
                if data_df.loc[m, share_label] in error_str:
                    data_df.loc[m, share_label] = 0
                if data_df.loc[m, shares] != 0 and not isna(data_df.loc[m, shares]):
                    data_df.loc[m, total_shares] = float(data_df.loc[m, share_label]) / (data_df.loc[m, shares] / 100)
                if data_df.loc[m, assets] != 0 and not isna(data_df.loc[m, assets]):
                    data_df.loc[m, total_assets] = float(data_df.loc[m, share_label]) / (data_df.loc[m, assets] / 100)

                df = data_df[share_label]

            # and data_df.loc[m,'Date'][0:3] == data_df.loc[m+1,'Date'][0:3]
            date_list = []
            for m in range(data_df.shape[0]):

                date = data_df.loc[m, 'Date']
                if date not in date_list:
                    t = int(data_df.loc[m, 'Date'][5])
                    # if m < data_df.shape[0] :
                    for i in range(t):
                        if isnull(data_df.loc[m, annualized_label]):
                            data_df.loc[m, annualized_label] = 0
                        if (m+i) < data_df.shape[0]:
                            if not isnull(df[m + i]) and df[m + i] not in error_str and data_df.loc[m,'Date'][0:3] == data_df.loc[m+i,'Date'][0:3]:

                                data_df.loc[m,annualized_label] += float(df[m+i])
                        else:
                            break
                    data_df.loc[m, annualized_label] = round(data_df.loc[m, annualized_label], 2)
                    if data_df.loc[m, annualized_label] != 0:
                        date_list.append(date)

                    df1 = data_df[annualized_label]
                else:
                    break

            for m in range(data_df.shape[0]):
                if m < data_df.shape[0] - 4:
                    if not isnull(df1[m]) and not isnull(df1[m + 4]) and df1[m] not in error_str and df1[m + 4] not in error_str:
                        data_df.loc[m, annualized_yoy_label] = round(float(df1[m]) / float(df1[m + 4]) - 1, 2)

                    if not isnull(df[m]) and not isnull(df[m + 4]) and df[m] not in error_str and df[m + 4] not in error_str:
                        data_df.loc[m, yoy_label] = round(float(df[m]) / float(df[m + 4]) - 1, 2)

                if m < data_df.shape[0] - 1:
                    if not isnull(df1[m]) and not isnull(df1[m + 1]) and df1[m] not in error_str and df1[m + 1] not in error_str:
                        data_df.loc[m, annualized_qoq_label] = round(float(df1[m]) / float(df1[m + 1]) - 1, 2)

                    if not isnull(df[m]) and not isnull(df[m + 1]) and df[m] not in error_str and df[m + 1] not in error_str:
                        data_df.loc[m, qoq_label] = round(float(df[m]) / float(df[m + 1]) - 1, 2)
        return data_df

    def run(self):
        with pool_sema:
            self.reformat_excel()
            self.calculate()
            print("--- {} Done {} ---".format(self.symbol, time.ctime()))


if __name__ == '__main__':
    # TSX, 'Snp500_Ru1000', 'Shanghai_Shenzhen'
    countries = ['Snp500_Ru1000']
    FIRST_PATH = "Calculate/"
    print(time.ctime())

    if not os.path.exists(FIRST_PATH):
        os.mkdir(FIRST_PATH)
    max_connections = 5
    pool_sema = threading.BoundedSemaphore(max_connections)
    for country in countries:
        print("\n---", country, "start ---")

        excel_path = "./RawData/{}/".format(country)
        SECOND_PATH = FIRST_PATH + "{}/".format(country)
        if not os.path.exists(SECOND_PATH):
            os.mkdir(SECOND_PATH)

        thread_list = []
        qc_path = "QCReport/{}/".format(country)  # QCReport/TSX/
        files = sorted(os.listdir(qc_path))
        for file in files[:20]:
            print(file)
            thread_list.append(CalculateData(country, file, qc_path))
        for num, thread in enumerate(thread_list):
            thread.name = str(num) + " " + thread.symbol
            print("=== {} start threading ===".format(thread.symbol))
            thread.start()
        for thread in thread_list:
            thread.join()
    print(time.ctime())
