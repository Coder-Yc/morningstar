#! FOR sencond calculation and summary
import os
import datetime
import threading
import re
from pandas import read_excel, ExcelWriter
from openpyxl.styles import Alignment, numbers
import pandas as pd
import time
from pandas import DataFrame, isnull, isna
from numpy import nan, nan_to_num, float64, int64
from threading import Thread

# 总结类 总结数据做最后的计算 把所有的股票都合并到一个excel文件里统为一个模版
# Output: ./result/SumReport_"date".xlsx
class SumClass(Thread):
    def __init__(self, symbol, funds_df, institutions_df):
        super(SumClass, self).__init__()
        added_heads = ["Symbol", "Date", "Source"]
        self.labels = ["OwnershipData", "ConcentratedOwners", "Buyers", "Sellers"]
        self.labels_1 = ["OwnershipData", "ConcentratedOwners", "Buyers", "Sellers"]
        self.symbol = symbol
        self.excel_file = excel_file
        self.excel_path = excel_path
        self.heads_adj, self.label_set , self.label_set_1= self.create_heads(self.excel_file,self.excel_path )
        self.file_df = DataFrame(columns=added_heads + self.heads_adj)
        self.funds_df = funds_df
        self.institutions_df = institutions_df


    @staticmethod
    def create_heads(excel_file,excel_path):
        # 获取
        labels = ["OwnershipData", "ConcentratedOwners", "Buyers", "Sellers"]
        labels_1 = ["OwnershipData", "ConcentratedOwners", "Buyers", "Sellers"]
        main_heads = ["Annualized", "Annualized_YoY", "Annualized_QoQ", "YoY", "QoQ", "CurrentShares",
                      "Fund/Institutions_TotalShares", "Fund/Institutions_TotalAssets",
                      "TotalSharesHeld", "TotalAssets", "ChangeAmount", "ChangePercentage"]
        a_labels = ["Annualized", "Annualized_YoY", "Annualized_QoQ", "YoY", "QoQ"]
        x1 = pd.ExcelFile(excel_path + excel_file)
        data = x1.sheet_names
        if 'Funds_OwnershipData' not in data:
            labels.remove('OwnershipData')
        if 'Funds_ConcentratedOwners' not in data:
            labels.remove('ConcentratedOwners')
        if 'Funds_Buyers' not in data:
            labels.remove('Buyers')
        if 'Funds_Sellers' not in data:
            labels.remove('Sellers')

        if 'Institutions_OwnershipData' not in data:
            labels_1.remove('OwnershipData')
        if 'Institutions_ConcentratedOwners' not in data:
            labels_1.remove('ConcentratedOwners')
        if 'Institutions_Buyers' not in data:
            labels_1.remove('Buyers')
        if 'Institutions_Sellers' not in data:
            labels_1.remove('Sellers')

        label_set = []
        heads_adj = []
        label_set_1 = []
        for a_label in a_labels:
            for label in labels:
                label_set.append(label + "_" + a_label)
        for a_label in a_labels:
            for label in labels_1:
                label_set_1.append(label + "_" + a_label)
        for label in labels:
            for head in main_heads:
                heads_adj.append(label + "_" + head)
        return heads_adj, label_set,label_set_1

    @staticmethod
    def compare(values):
        values = [v for v in values if not isna(v)]
        values = list(set(values))
        value = 0.0
        if len(values) == 1:
            value = values[0]
        else:
            for v in values:
                value = max(v, value)
        return value

    def format2excel(self):
        funds_df = self.funds_df.set_index("Date")
        institutions_df = self.institutions_df.set_index("Date")
        funds_date = self.funds_df.loc[:, "Date"].to_list()
        institutions_date = self.institutions_df.loc[:, "Date"].to_list()
        dates = list(set(funds_date).union(set(institutions_date)))
        if nan in dates:
            dates.remove(nan)
        dates.sort()
        dates.reverse()
        df_1 = {}
        df_2 = {}

        x1 = pd.ExcelFile(excel_path + excel_file)
        data = x1.sheet_names
        if 'Funds_OwnershipData' not in data:
            self.labels.remove('OwnershipData')
        if 'Funds_ConcentratedOwners' not in data:
            self.labels.remove('ConcentratedOwners')
        if 'Funds_Buyers' not in data:
            self.labels.remove('Buyers')
        if 'Funds_Sellers' not in data:
            self.labels.remove('Sellers')

        if 'Institutions_OwnershipData' not in data:
            self.labels_1.remove('OwnershipData')
        if 'Institutions_ConcentratedOwners' not in data:
            self.labels_1.remove('ConcentratedOwners')
        if 'Institutions_Buyers' not in data:
            self.labels_1.remove('Buyers')
        if 'Institutions_Sellers' not in data:
            self.labels_1.remove('Sellers')
        # print(self.labels)
        for label in self.labels:

            funds_df_ = self.funds_df
            funds_df_index = [column for column in funds_df_]
            for n in range(funds_df_.shape[0]):
                if label + "_TotalSharesHeld" in funds_df_index:
                    if isna(funds_df_.loc[n, label + "_TotalSharesHeld"]):
                        funds_df_ = funds_df_.drop(n)
                else:
                    continue
            df_1[label] = funds_df_
        for label in self.labels_1:
            institutions_df_ = self.institutions_df
            for n in range(institutions_df_.shape[0]):
                if isna(institutions_df_.loc[n, label + "_TotalSharesHeld"]):
                    institutions_df_ = institutions_df_.drop(n)
            df_2[label] = institutions_df_



        for date in dates:
            data_df = DataFrame()

            for head in heads_adj:
                data_df.loc[0, head] = nan
                data_df.loc[1, head] = nan
                data_df.loc[2, head] = nan
                lab = head.split("_")[0]
                if "Fund/Institutions_TotalShares" in head:
                    if df_1[lab].loc[:, head].empty:
                        data_df.loc[0, head] = nan
                    else:
                        data_df.loc[0, head] = df_1[lab].loc[:, head].reset_index().loc[0, head]
                    if df_2[lab].loc[:, head].empty:
                        data_df.loc[1, head] = nan
                    else:
                        data_df.loc[1, head] = data_df.loc[0, head]
                        data_df.loc[2, head] = data_df.loc[0, head]
                    continue
                if "Fund/Institutions_TotalAssets" in head:
                    data_df.loc[0, head] = sum(list(set([v for v in df_1[lab].loc[:, head].to_list() if not isna(v)])))
                    data_df.loc[1, head] = sum(list(set([v for v in df_2[lab].loc[:, head].to_list() if not isna(v)])))
                    data_df.loc[2, head] = nan_to_num(data_df.loc[0, head]) + nan_to_num(data_df.loc[1, head])
                    continue
                if head not in self.label_set:
                    # funds
                    if date in funds_date:
                        if type(funds_df.loc[date, head]) in [float, float64, int64]:
                            data_df.loc[0, head] = nan_to_num(funds_df.loc[date, head])
                        else:
                            data_df.loc[0, head] = 0
                            for val in list(set(funds_df.loc[date, head].to_list())):
                                if val not in ["None", " None"]:
                                    data_df.loc[0, head] += nan_to_num(float(val))
                if head not in self.label_set_1:
                    # institutions
                    if date in institutions_date and head in institutions_df.columns.to_list():
                        if type(institutions_df.loc[date, head]) in [float, float64, int64]:
                            data_df.loc[1, head] = nan_to_num(institutions_df.loc[date, head])
                        else:
                            data_df.loc[1, head] = 0
                            for val in list(set(institutions_df.loc[date, head].to_list())):
                                if val not in ["None", " None"]:
                                    data_df.loc[1, head] += nan_to_num(float(val))
                    # total
                data_df.loc[2, head] = nan_to_num(data_df.loc[0, head]) + nan_to_num(data_df.loc[1, head])

            data_df.loc[0, "Symbol"] = self.symbol
            data_df.loc[0, "Source"] = "Funds"
            data_df.loc[0, "Date"] = date
            data_df.loc[1, "Symbol"] = self.symbol
            data_df.loc[1, "Source"] = "Institutions"
            data_df.loc[1, "Date"] = date
            data_df.loc[2, "Symbol"] = self.symbol
            data_df.loc[2, "Source"] = "Total"
            data_df.loc[2, "Date"] = date
            self.file_df = self.file_df.append(data_df)

    def sum_calculate(self):


        if self.file_df.empty:
            return self.file_df

        if set(self.file_df["Date"].to_list()).__len__() == 1:
            total_df = DataFrame().append(self.file_df.loc[2], ignore_index=True)
            funds_df = DataFrame().append(self.file_df.loc[0], ignore_index=True)
            institutions_df = DataFrame().append(self.file_df.loc[1], ignore_index=True)
        else:
            total_df = self.file_df.set_index("Source").loc["Total"].reset_index()
            funds_df = self.file_df.set_index("Source").loc["Funds"].reset_index()
            institutions_df = self.file_df.set_index("Source").loc["Institutions"].reset_index()

        for label in self.labels:
            # funds
            self.summary(funds_df, label)
            # institutions
            self.summary(institutions_df, label)
            # total
            shares_label = label + "_CurrentShares"
            for n in range(total_df.shape[0]):
                total_df.loc[n, shares_label] = round(nan_to_num(funds_df.loc[n, shares_label]) +
                                                      nan_to_num(institutions_df.loc[n, shares_label]), 2)
            self.summary(total_df, label)

        temp_df = funds_df.append(institutions_df, ignore_index=True).append(total_df, ignore_index=True)
        dates = list(set(temp_df.loc[:, "Date"]))
        self.file_df = self.file_df.set_index("Date")
        for date in dates:
            self.file_df.loc[date, self.file_df.columns] = temp_df.set_index("Date").loc[date, self.file_df.columns]
        self.file_df = self.file_df.reset_index()

    @staticmethod
    def summary(data_df, label):
        annualized_label = label + "_Annualized"
        annualized_yoy_label = label + "_Annualized_YoY"
        annualized_qoq_label = label + "_Annualized_QoQ"
        yoy_label = label + "_YoY"
        qoq_label = label + "_QoQ"
        shares_label = label + "_CurrentShares"
        shares = label + "_TotalSharesHeld"
        assets = label + "_TotalAssets"
        total_shares = label + "_Fund/Institutions_TotalShares"
        total_assets = label + "_Fund/Institutions_TotalAssets"



        for m in range(data_df.shape[0]):
            # % Shares
            data_df.loc[m, shares] = 0
            if data_df.loc[m, total_shares] != 0 and not isna(data_df.loc[m, total_shares]):
                data_df.loc[m, shares] = round(data_df.loc[m, shares_label] / data_df.loc[m, total_shares] *100, 2)
            data_df.loc[m, assets] = 0
            if data_df.loc[m, total_assets] != 0 and not isna(data_df.loc[m, total_assets]):
                data_df.loc[m, assets] = round(data_df.loc[m, shares_label] / data_df.loc[m, total_assets] *100, 2)

        df = data_df[shares_label]

        # Annualized
        for m in range(data_df.shape[0]):
            if m < data_df.shape[0]-3:
                for i in range(4):
                    data_df.loc[m, annualized_label] = nan_to_num(data_df.loc[m, annualized_label])
                    data_df.loc[m, annualized_label] += float(nan_to_num(df[m + i]))

        df1 = data_df[annualized_label]

        for m in range(data_df.shape[0]):
            if m < data_df.shape[0] - 4:
                # Annualized YoY
                if not isnull(df1[m]) and not isnull(df1[m + 4]) and df1[m + 4] != 0:
                    data_df.loc[m, annualized_yoy_label] = round(df1[m] / df1[m + 4] - 1, 2)
                # YoY
                if not isnull(df[m]) and not isnull(df[m + 4]) and df[m + 4] != 0:
                    data_df.loc[m, yoy_label] = round(df[m] / df[m + 4] - 1, 2)

            if m < data_df.shape[0] - 1:
                # Annualized QoQ
                if not isnull(df1[m]) and not isnull(df1[m + 1]) and df1[m + 1] != 0:
                    data_df.loc[m, annualized_qoq_label] = round(df1[m] / df1[m + 1] - 1, 2)
                # QoQ
                if not isnull(df[m]) and not isnull(df[m + 1]) and df[m + 1] != 0:
                    data_df.loc[m, qoq_label] = round(df[m] / df[m + 1] - 1, 2)

    def run(self):
        with pool_sema:
            self.format2excel()
            self.sum_calculate()
            print("=== {} done at {} ===".format(self.symbol, time.ctime()))


if __name__ == '__main__':
    columns = ['Symbol', 'Date', 'Source', 'OwnershipData_Annualized', 'OwnershipData_Annualized_YoY',
               'OwnershipData_Annualized_QoQ','OwnershipData_YoY', 'OwnershipData_QoQ',
               'OwnershipData_CurrentShares', 'OwnershipData_TotalSharesHeld', 'OwnershipData_TotalAssets',
               'OwnershipData_Fund/Institutions_TotalShares', 'OwnershipData_Fund/Institutions_TotalAssets',
               'OwnershipData_ChangeAmount', 'OwnershipData_ChangePercentage', 'ConcentratedOwners_Annualized',
               'ConcentratedOwners_Annualized_YoY', 'ConcentratedOwners_Annualized_QoQ', 'ConcentratedOwners_YoY',
               'ConcentratedOwners_QoQ', 'ConcentratedOwners_CurrentShares',
               'ConcentratedOwners_Fund/Institutions_TotalShares',
               'ConcentratedOwners_Fund/Institutions_TotalAssets', 'ConcentratedOwners_TotalSharesHeld',
               'ConcentratedOwners_TotalAssets', 'ConcentratedOwners_ChangeAmount',
               'ConcentratedOwners_ChangePercentage', 'Buyers_Annualized', 'Buyers_Annualized_YoY',
               'Buyers_Annualized_QoQ', 'Buyers_YoY', 'Buyers_QoQ', 'Buyers_CurrentShares',
               'Buyers_Fund/Institutions_TotalShares', 'Buyers_Fund/Institutions_TotalAssets',
               'Buyers_TotalSharesHeld', 'Buyers_TotalAssets', 'Buyers_ChangeAmount', 'Buyers_ChangePercentage',
               'Sellers_Annualized', 'Sellers_Annualized_YoY', 'Sellers_Annualized_QoQ', 'Sellers_YoY',
               'Sellers_QoQ', 'Sellers_CurrentShares', 'Sellers_Fund/Institutions_TotalShares',
               'Sellers_Fund/Institutions_TotalAssets', 'Sellers_TotalSharesHeld', 'Sellers_TotalAssets',
               'Sellers_ChangeAmount', 'Sellers_ChangePercentage']
    global excel_file,excel_path
    #'TSX' , 'Snp500_Ru1000', 'Shanghai_Shenzhen'
    countries = ['TSX']
    Frequency = "Quarterly"
    added_heads = ["Symbol", "Date", "Source"]
    print("# Sum # Start at:", time.ctime())
    if str(datetime.datetime.now().month).__len__() == 1:
        m = "0" + str(datetime.datetime.now().month)
    else:
        m = str(datetime.datetime.now().month)
    save_path = "./result/SumReport_{}.xlsx".format(str(datetime.datetime.now().year) + m)
    writer = ExcelWriter(save_path, engine="openpyxl")
    max_connections = 5
    pool_sema = threading.BoundedSemaphore(max_connections)
    for country in countries:
        print("====", country, time.ctime(), "start ====")
        files = os.listdir("Calculate/{}/".format(country))
        datas = {}
        thread_list = []
        i = 1
        for file in files:
            excel_file = file.replace('Calculate', 'Download')
            excel_path = "./RawData/{}/".format(country)
            if i == 1:
                heads_adj, label_set, label_set_1 = SumClass.create_heads(excel_file,excel_path)

                main_df = DataFrame(columns=added_heads + heads_adj)

            symbol = re.sub(r"(Calculate_)|(.xlsx)", "", file)
            print("------>", symbol, "ready <------")
            i += 1
            try:
                funds_df = read_excel("Calculate/{}/".format(country) + file, sheet_name="Funds")
                institutions_df = read_excel("Calculate/{}/".format(country) + file, sheet_name="Institutions")
                thread_list.append(SumClass(symbol, funds_df, institutions_df))
            except:
                thread_list.append(SumClass(symbol, funds_df, institutions_df))
        print("=== start threading ===")
    for num, thread in enumerate(thread_list):
        thread.name = str(num) + " " + thread.symbol
        while threading.active_count()>500:
            time.sleep(10)
        thread.start()
    for thread in thread_list:
        thread.join()
    print("=== end threading ===")
    for thread in thread_list:
        main_df = main_df.append(thread.file_df, ignore_index=True)

    main_df.to_excel(writer, sheet_name=country, index=False, columns=added_heads + heads_adj)
    wb = writer.book
    wb._write_only = False
    wb._read_only = False
    for i in [0, 1, 6, 7]:
        for j in range(4):
            col = len(added_heads) + 1 + i + 12 * j
            if wb[country].cell(row=1, column=col).value != None:
                wb[country].cell(row=1, column=col).value += "(K)"
            else:
                break
    for cell in wb[country][1]:
        cell.alignment = Alignment(wrap_text=True)
    for col in wb[country]["D":"AY"]:
        for cell in col:
            cell.number_format = numbers.FORMAT_NUMBER_00
    wb[country].freeze_panes = "D2"
    writer.book = wb
    writer.save()
    writer.close()
    print("# Sum # End at:", time.ctime())
    print("file start redeal:")
    df_redeal = pd.read_excel("/Users/yangchong/Desktop/work/morningstar/result/SumReport_{}.xlsx".format(str(datetime.datetime.now().year) + m))
    df_redeal.columns =columns
    df_redeal.to_excel('SumReport_{}_{}.xlsx'.format(str(datetime.datetime.now().year)+m,country), sheet_name='{}'.format(country), index=None)
    print("file End redeal:")