# 由于在Download里面下载的文件 正常来说是八个sheet 由于一些股票会没有数据 导致8个sheet不全 会导致最后做sum总结的时候出现bug
# 这个文件里代码的意义就在于补全缺失的sheet把没有的数据统统按照0来填入 最终返回一个完整excel表
# 值得注意的是 这里是需要拷贝一份Calculate跑出来的的文件夹到你的桌面 具体路径看下文

import pandas as pd
import os
# 选择你所需要的国家
countries = ['Snp500_Ru1000']
for country in countries:
    excel_path_Ca = "./Calculate/{}/".format(country)
    excel_files_QC = os.listdir(excel_path_Ca)
    excel_path = "./RawData/{}/".format(country)
    head = ['_Annualized','_Annualized_YoY','_Annualized_QoQ','_YoY','_QoQ','_Fund/Institutions_TotalShares','_Fund/Institutions_TotalAssets','_TotalSharesHeld','_TotalAssets','_CurrentShares','_ChangeAmount','_ChangePercentage','_StarRating']
    head_1 = ['_Annualized', '_Annualized_YoY', '_Annualized_QoQ', '_YoY', '_QoQ', '_Fund/Institutions_TotalShares','_Fund/Institutions_TotalAssets', '_TotalSharesHeld', '_TotalAssets', '_CurrentShares', '_ChangeAmount']
    labels_ = ["OwnershipData", "ConcentratedOwners", "Buyers", "Sellers"]
    data =['Funds_OwnershipData', 'Institutions_OwnershipData', 'Funds_ConcentratedOwners','Institutions_ConcentratedOwners','Funds_Buyers', 'Institutions_Buyers', 'Funds_Sellers', 'Institutions_Sellers']
    for file in excel_files_QC:
        labels = ["OwnershipData", "ConcentratedOwners", "Buyers", "Sellers"]
        labels_1 = ["OwnershipData", "ConcentratedOwners", "Buyers", "Sellers"]
        print(file)
        excel_file = file.replace('Calculate', 'Download')
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
        try:
            # funds
            # excel_的路径为你拷贝Calculate跑出来的文件夹的位置
            excel_ = "/Users/yangchong/Desktop/{}/{}".format(country,file)
            excel = "/Users/yangchong/Desktop/work/morningstar/Calculate/{}/{}".format(country,file)
            write = pd.ExcelWriter(excel)
            wb = pd.read_excel(excel_, sheet_name='Funds')
            col_name = wb.columns.tolist()
            if labels != labels_:
                if 'OwnershipData' not in labels:
                    for i,head_ in enumerate(head):
                        index = col_name.index('Source') + (i+1)
                        col_name.insert(index,'OwnershipData'+head_)
                        df1 = wb.reindex(columns=col_name)
                        df1['OwnershipData'+head_].fillna(0,inplace=True)
                        df1.to_excel(write, sheet_name='Funds', index=False)
                if 'ConcentratedOwners' not in labels:
                    for i,head_ in enumerate(head):
                        index = col_name.index('OwnershipData_StarRating') + (i+1)
                        col_name.insert(index,'ConcentratedOwners'+head_)
                        df1 = wb.reindex(columns=col_name)
                        # df1['ConcentratedOwners'+head_].fillna(0, inplace=True)
                        df1.to_excel(write, sheet_name='Funds', index=False)
                if 'Buyers' not in labels:
                    for i,head_ in enumerate(head):
                        index = col_name.index('ConcentratedOwners_StarRating') + (i+1)
                        col_name.insert(index,'Buyers'+head_)
                        df1 = wb.reindex(columns=col_name)
                        # df1['Buyers'+head_].fillna(0, inplace=True)
                        df1.to_excel(write,  sheet_name='Funds', index=False)
                if 'Sellers' not in labels:
                    for i,head_ in enumerate(head):
                        index = col_name.index('Buyers_StarRating') + (i+1)
                        col_name.insert(index,'Sellers'+head_)
                        df1 = wb.reindex(columns=col_name)
                        # df1['Sellers'+head_].fillna(0, inplace=True)
                        df1.to_excel(write, sheet_name='Funds',index =False)
                for lebel in labels_:
                    for head_ in head:

                        df1[lebel+head_].fillna(0,inplace=True)
                    df1.to_excel(write,sheet_name='Funds',index= False)
            else:
                df1 = wb.reindex(columns=col_name)
                df1.to_excel(write, sheet_name='Funds', index=False)
                for lebel in labels_:
                    for head_ in head:

                        df1[lebel+head_].fillna(0,inplace=True)
                    df1.to_excel(write,sheet_name='Funds',index= False)

        #     Institutions
            wb_Institutions = pd.read_excel(excel_,sheet_name='Institutions')
            col_name_1 = wb_Institutions.columns.tolist()

            if labels_1 != labels_:
                if 'OwnershipData' not in labels_1:
                    for i,head_ in enumerate(head_1):
                        index = col_name_1.index('Source') + (i+1)
                        col_name_1.insert(index,'OwnershipData'+head_)
                        df2 = wb_Institutions.reindex(columns=col_name_1)
                        df2['OwnershipData'+head_].fillna(0, inplace=True)
                        df2.to_excel(write, sheet_name='Institutions', index=False)
                if 'ConcentratedOwners' not in labels_1:
                    for i,head_ in enumerate(head_1):
                        index = col_name_1.index('OwnershipData_ChangeAmount') + (i+1)
                        col_name_1.insert(index,'ConcentratedOwners'+head_)
                        df2 = wb_Institutions.reindex(columns=col_name_1)
                        # df2['ConcentratedOwners'+head_].fillna(0, inplace=True)
                        df2.to_excel(write, sheet_name='Institutions', index=False)
                if 'Buyers' not in labels_1:
                    for i,head_ in enumerate(head_1):
                        index = col_name_1.index('ConcentratedOwners_ChangeAmount') + (i+1)
                        col_name_1.insert(index,'Buyers'+head_)
                        df2 = wb_Institutions.reindex(columns=col_name_1)
                        # df2['Buyers'+head_].fillna(0, inplace=True)
                        df2.to_excel(write, sheet_name='Institutions', index=False)
                if 'Sellers' not in labels_1:
                    for i,head_ in enumerate(head_1):
                        index = col_name_1.index('Buyers_ChangeAmount') + (i+1)
                        col_name_1.insert(index,'Sellers'+head_)
                        df2 = wb_Institutions.reindex(columns=col_name_1)
                        # df2['Sellers'+head_].fillna(0, inplace=True)
                        df2.to_excel(write,sheet_name='Institutions',index =False)
                for lebel in labels_:
                    for head_ in head_1:
                        df2[lebel+head_].fillna(0,inplace=True)
                    df2.to_excel(write,sheet_name='Institutions',index= False)

            else:
                df2 = wb_Institutions.reindex(columns=col_name_1 )
                df2.to_excel(write, sheet_name='Institutions', index=False)
                for lebel in labels_:
                    for head_ in head_1:
                        df2[lebel+head_].fillna(0,inplace=True)
                    df2.to_excel(write,sheet_name='Institutions',index= False)

            write.save()

        except:
            continue














