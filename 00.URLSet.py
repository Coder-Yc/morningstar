import json
import datetime
import time
import requests
import os
from pandas import ExcelWriter, read_excel
from threading import Thread
from requests import adapters

# 定义一个测试类URLThread,传入股票代码以及国家，测试网址是否能打开返回正常数据，实现多线程下载

class URLThread(Thread):
    origin_url = "https://www.morningstar.com/stocks/{}/{}/ownership"
    classifications = {
        'Shanghai_Shenzhen': ['xshe', 'xshg'],
        'Snp500_Ru1000': ['xnys', 'xnas', 'pinx', 'xdus'],
        'TSX': ['xtse', 'xbkk', 'xmex']
    }

    def __init__(self, symbol, country):
        super(URLThread, self).__init__()
        self.country = country
        self.symbol = symbol.lower()
        self.owner_url = ""

    def run(self):
        classify =  ['xtse', 'xbkk', 'xmex']
        for classify in self.classifications[self.country]:
            url = self.origin_url.format(classify, self.symbol)
            response = requests.get(url, headers={'Connection': 'close'})
            # if the symbol is available
            if response.status_code == 200:
                self.owner_url = url
                print(self.symbol, self.owner_url, "done")
                response.close()
                break
            response.close()
            time.sleep(5)

# 下载函数，对于不同国家的股票实现分类，便于获取股票的morningstar的网址位置
# 获取每支股票Ownership页面的网址，网址以字典形式保存于json文件之中，一般只需要在项目开始时运行一次，生成的文件将用于后续工作
# Output: ./util/Owner_URLs_"xx".json
def setting_url():
    pathtxt = './util/Owner_URLs_{}.json'
    if not os.path.exists(pathtxt):
        os.mkdir(pathtxt)
    for country in countries:
        ''' extract 'to be downloaded' symbols '''
        owner_urls = {}
        writer = ExcelWriter("./master/Master_{}_{}.xlsx".format(country, str(datetime.date.today().year) + str(datetime.date.today().month)), engine='openpyxl')
        master_sheet_origin_download = read_excel(master_symbol_main_file, sheet_name=country)
        download_items = master_sheet_origin_download.loc[master_sheet_origin_download['currently use'] == 'yes'].copy()
        download_items.to_excel(writer, sheet_name=country, index=False, encoding='utf-8')
        writer.save()
        symbols = download_items['MS_Symbol']
        print(" $ Extracting done.")

        ''' pattern owner urls '''
        thread_list = []
        non_exist_symbols = []
        exist_urls = []
        url_path = "./util/Owner_URLs_{}.json".format(country)
        if os.path.exists(url_path):
            with open(url_path, "r", encoding="utf-8") as f:
                exist_urls = json.load(f)  # dict
            print("    Existing symbols:", len(exist_urls))
            # symbol = 'ZZZ'
            for symbol in symbols[:20]:
                symbol = str(symbol).lower()
                if country == 'Shanghai_Shenzhen':
                    symbol = symbol.zfill(6)
                if symbol in exist_urls.keys():
                    if exist_urls[symbol] == "":
                        non_exist_symbols.append(symbol)
                else:
                    non_exist_symbols.append(symbol)
        else:
            non_exist_symbols = symbols

        for symbol in non_exist_symbols:
            symbol = str(symbol)
            if country == 'Shanghai_Shenzhen':
                symbol = symbol.zfill(6)
            thread_list.append(URLThread(symbol, country))
        for i, url_thread in enumerate(thread_list):
            print(i,url_thread.symbol, "start")
            if i % 5 == 0 and i != 0:
                time.sleep(15)
            if i % 10 == 0 and i != 0:
                time.sleep(30)
            url_thread.start()
        for url_thread in thread_list:
            owner_urls[url_thread.symbol] = url_thread.owner_url
            owner_urls.update(exist_urls)
            print("Update:", len(owner_urls))
            with open("./util/Owner_URLs_{}.json".format(country), "w", encoding="utf-8") as f:
                json.dump(owner_urls, f, indent=4)


if __name__ == '__main__':
    adapters.DEFAULT_RETRIES = 5
    master_symbol_main_file = "./master/master_symbol_v1.6_2020.7.xlsx"  # use latest master sheet
    # countries可以修改它的值 一个一个国家的跑
    countries = ['TSX', 'Snp500_Ru1000', 'Shanghai_Shenzhen']
    setting_url()
