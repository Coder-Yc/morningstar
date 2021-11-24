# FOR getting datas
import re
import os
import requests
import time
import json
from threading import Thread
from collections import defaultdict
from requests import adapters
from util.user_agent import get_user_agent_pc
from pandas import  ExcelWriter,DataFrame



adapters.DEFAULT_RETRIES = 10

# 定义一个下载类，传入的参数为symbol，url以及country，利用request函数获取页面信息，以及我们所需要的内容
# 将所下载的内容以xsxl的形式保存下来
# Output: ./RawData/"Country"/Download_"xx".xlsx

class GetMorningStarData(Thread):
    objects = ['OwnershipData', 'ConcentratedOwners', 'Buyers', 'Sellers']
    funds_heads = ["Name", "TotalSharesHeld", "TotalAssets", "CurrentShares", "ChangeAmount", "ChangePercentage", "Date", "StarRating", "Trend"]
    institutions_heads = ["Name", "TotalSharesHeld", "TotalAssets", "CurrentShares", "ChangeAmount", "Date", "Trend"]

    def __init__(self, symbol, base_url, country):
        super(GetMorningStarData, self).__init__()
        self.symbol = symbol
        self.country = country
        self.pattern = r''
        self.id = ''
        self.owner_url_funds = ""
        self.owner_url_institutions = ""
        self.base_url = base_url
        self.funds_data_list = defaultdict(list)
        self.institutions_data_list = defaultdict(list)
        self.funds_base_url = "https://api-global.morningstar.com/sal-service/v1/stock/ownership/v1/{}/{}/mutualfund/20/data?locale=en&clientId=MDC&benchmarkId=category&version=3.31.0"
        self.institutions_base_url = "https://api-global.morningstar.com/sal-service/v1/stock/ownership/v1/{}/{}/institution/20/data?locale=en&clientId=MDC&benchmarkId=category&version=3.31.0"

    def parse_objects(self):
        for obj in self.objects:
            base_response = requests.get(self.base_url)
            time.sleep(1)
            if base_response.status_code == 200:
                self.pattern = re.compile(r'byId:{"?[0-9A-Z]+"?')
                self.id = re.sub('byId:{', '', self.pattern.findall(base_response.text)[0]).replace('"', '')
                base_response.close()
                self.owner_url_funds = self.funds_base_url.format(self.id, obj)
                self.owner_url_institutions = self.institutions_base_url.format(self.id, obj)
                funds_response = requests.get(self.owner_url_funds, headers={
                    "accept": "application/json, text/plain, */*",
                    "accept-encoding": "gzip, deflate, br",
                    "accept-language": "zh-CN,zh;q=0.9",
                    "apikey": "lstzFDEOhfFNMLikKa0am9mgEKLBl49T",
                    "cookie": "_ga=GA1.2.169521830.1585833584; _gcl_au=1.1.774006094.1600340106; ELOQUA=GUID=E6AA5AC337274FBF9E0907018BA3C2A0; mid=12293387875223924076; __utmz=172984700.1600858791.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); fp=015160085879271333; ms-rme=QVFJQ2ErSmZ5N0xYd2dSeG9IQ0xpMUxVZDlNUnRXM1BLQ1RyZVJyWTJrM0VtSFdjcnlFaVVWcFJCZ3IrY0Fmem1QZ2NCWFRWQStWK09yY3cxQmpEdUZSazJRPT0; iridium=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InVzZXJUeXBlIjoicHJlbWl1bSIsImNvbnRlbnRUeXBlIjoibk5zR2ROM1JFT25QTWxLRFNoT1lqbGs2VllpRVZMU2RwZnBYQW03bzJUaz0iLCJpc0Fkdmlzb3IiOnRydWV9LCJpYXQiOjE2MDEwMjE2ODgsImV4cCI6MTYwODc5NzY4NH0.uA1gKV3iXLMlAdMd8jf5-7SvE-SbbVoWIjji-PWN1EM; __utma=172984700.169521830.1585833584.1601297767.1601367372.3; _uetsid=1c5396c4acb9fee1720ff2e0634de65f; _uetvid=9268e8f91c7b7e2d5ddd0e83f87468d6; _gid=GA1.2.602477185.1601543118",
                    "origin": "https://www.morningstar.com",
                    "referer": "https://www.morningstar.com/stocks/xnas/aapl/ownership",
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-site",
                    "user-agent":  get_user_agent_pc(),
                    "x-api-realtime-e": "eyJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.XmuAS3x5r-0MJuwLDdD4jNC6zjsY7HAFNo2VdvGg6jGcj4hZ4NaJgH20ez313H8An9UJrsUj8ERH0R8UyjQu2UGMUnJ5B1ooXFPla0LQEbN_Em3-IG84YPFcWVmEgcs1Fl2jjlKHVqZp04D21UvtgQ4xyPwQ-QDdTxHqyvSCpcE.ACRnQsNuTh1K_C9R.xpLNZ8Cc9faKoOYhss1CD0A4hG4m0M7-LZQ0fISw7NUHwzQs2AEo9ZXfwOvAj1fCbcE96mbKQo8gr7Oq1a2-piYXM1X5yNMcCxEaYyGinpnf6PGqbdr6zbYZdqyJk0KrxWVhKSQchLJaLGJOts4GlpqujSqJObJQcWWbkJQYKG9K7oKsdtMAKsHIVo5-0BCUbjKVnHJNsYwTsI7xn2Om8zGm4A.nBOuiEDssVFHC_N68tDjVA",
                    "x-api-requestid": "9ade0dc1-f01e-dc5b-ea46-1dea59b3fee9",
                    "x-sal-contenttype": "nNsGdN3REOnPMlKDShOYjlk6VYiEVLSdpfpXAm7o2Tk=",
                    "Connection": "close"
                })
                time.sleep(1)
                institutions_response = requests.get(self.owner_url_institutions, headers={
                    "accept": "application/json, text/plain, */*",
                    "accept-encoding": "gzip, deflate, br",
                    "accept-language": "zh-CN,zh;q=0.9",
                    "apikey": "lstzFDEOhfFNMLikKa0am9mgEKLBl49T",
                    "cookie": "_ga=GA1.2.169521830.1585833584; _gcl_au=1.1.774006094.1600340106; ELOQUA=GUID=E6AA5AC337274FBF9E0907018BA3C2A0; mid=12293387875223924076; __utmz=172984700.1600858791.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); fp=015160085879271333; ms-rme=QVFJQ2ErSmZ5N0xYd2dSeG9IQ0xpMUxVZDlNUnRXM1BLQ1RyZVJyWTJrM0VtSFdjcnlFaVVWcFJCZ3IrY0Fmem1QZ2NCWFRWQStWK09yY3cxQmpEdUZSazJRPT0; iridium=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InVzZXJUeXBlIjoicHJlbWl1bSIsImNvbnRlbnRUeXBlIjoibk5zR2ROM1JFT25QTWxLRFNoT1lqbGs2VllpRVZMU2RwZnBYQW03bzJUaz0iLCJpc0Fkdmlzb3IiOnRydWV9LCJpYXQiOjE2MDEwMjE2ODgsImV4cCI6MTYwODc5NzY4NH0.uA1gKV3iXLMlAdMd8jf5-7SvE-SbbVoWIjji-PWN1EM; __utma=172984700.169521830.1585833584.1601297767.1601367372.3; _uetsid=1c5396c4acb9fee1720ff2e0634de65f; _uetvid=9268e8f91c7b7e2d5ddd0e83f87468d6; _gid=GA1.2.602477185.1601543118",
                    "origin": "https://www.morningstar.com",
                    "referer": "https://www.morningstar.com/stocks/xnas/aapl/ownership",
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-site",
                    "user-agent":  get_user_agent_pc(),
                    "x-api-realtime-e": "eyJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.XmuAS3x5r-0MJuwLDdD4jNC6zjsY7HAFNo2VdvGg6jGcj4hZ4NaJgH20ez313H8An9UJrsUj8ERH0R8UyjQu2UGMUnJ5B1ooXFPla0LQEbN_Em3-IG84YPFcWVmEgcs1Fl2jjlKHVqZp04D21UvtgQ4xyPwQ-QDdTxHqyvSCpcE.ACRnQsNuTh1K_C9R.xpLNZ8Cc9faKoOYhss1CD0A4hG4m0M7-LZQ0fISw7NUHwzQs2AEo9ZXfwOvAj1fCbcE96mbKQo8gr7Oq1a2-piYXM1X5yNMcCxEaYyGinpnf6PGqbdr6zbYZdqyJk0KrxWVhKSQchLJaLGJOts4GlpqujSqJObJQcWWbkJQYKG9K7oKsdtMAKsHIVo5-0BCUbjKVnHJNsYwTsI7xn2Om8zGm4A.nBOuiEDssVFHC_N68tDjVA",
                    "x-api-requestid": "9ade0dc1-f01e-dc5b-ea46-1dea59b3fee9",
                    "x-sal-contenttype": "nNsGdN3REOnPMlKDShOYjlk6VYiEVLSdpfpXAm7o2Tk=",
                    "Connection": "close"
                })
                time.sleep(1)
                if funds_response.status_code == 200 and institutions_response.status_code == 200:
                    funds_rows = funds_response.json()["rows"]
                    institutions_rows = institutions_response.json()["rows"]
                    institutions_response.close()
                    funds_response.close()
                    for js in funds_rows:
                        if js["trend"]:
                            trends = [str(trend) for trend in js["trend"]]
                        else:
                            trends = []
                        funds_data = {
                            "name": js["name"],
                            "totalSharesHeld": js["totalSharesHeld"],
                            "totalAssets": js["totalAssets"],
                            "currentShares": js["currentShares"],
                            "changeAmount": js["changeAmount"],
                            "changePercentage": js["changePercentage"],
                            "date": js["date"],
                            "starRating": js["starRating"],
                            "trend": " ".join(trends)
                        }
                        self.funds_data_list[obj].append(funds_data)
                    for js in institutions_rows:
                        if js["trend"]:
                            trends = [str(trend) for trend in js["trend"]]
                        else:
                            trends = []
                        institutions_data = {
                            "name": js["name"],
                            "totalSharesHeld": js["totalSharesHeld"],
                            "totalAssets": js["totalAssets"],
                            "currentShares": js["currentShares"],
                            "changeAmount": js["changeAmount"],
                            "date": js["date"],
                            "trend": " ".join(trends)
                        }
                        self.institutions_data_list[obj].append(institutions_data)
    # 保存函数
    def save_to_excel(self):
        excel_path = "./RawData/{}/Download_{}.xlsx".format(self.country, self.symbol)
        with ExcelWriter(excel_path, engine="openpyxl") as writer:
            for obj in self.objects:
                funds_df = DataFrame(self.funds_data_list[obj])
                if not funds_df.empty:
                    funds_df["name"][len(funds_df["name"]) - 1] = "Total (for Top 20)"
                    for n, date in enumerate(list(funds_df["date"])):
                        if date:
                            funds_df["date"][n] = date.replace('T00:00:00.000', '')
                if not funds_df.empty:
                    funds_df.to_excel(excel_writer=writer, sheet_name='Funds_{}'.format(obj), index=False, header=self.funds_heads, encoding="utf-8")
                    # tab += 1

                institutions_df = DataFrame(self.institutions_data_list[obj])

                if not institutions_df.empty:
                    institutions_df["name"][len(institutions_df["name"]) - 1] = "Total (for Top 20)"
                    for n, date in enumerate(list(institutions_df["date"])):
                        if date:
                            institutions_df["date"][n] = date.replace('T00:00:00.000', '')
                if not institutions_df.empty:

                    institutions_df.to_excel(excel_writer=writer, sheet_name='Institutions_{}'.format(obj), index=False, header=self.institutions_heads, encoding="utf-8")
    def run(self):
        self.parse_objects()
        self.save_to_excel()

#启动函数
if __name__ == '__main__':
    # 'TSX', 'Snp500_Ru1000','Shanghai_Shenzhen
    countries = ['Shanghai_Shenzhen']
    print("# Download # begin at:", time.ctime())
    FIRST_PATH = "./RawData/"
    if not os.path.exists(FIRST_PATH):
        os.mkdir(FIRST_PATH)
    for country in countries:
        print("\n---", country, "start ---")
        SECOND_PATH = FIRST_PATH + "{}/".format(country)
        if not os.path.exists(SECOND_PATH):
            os.mkdir(SECOND_PATH)

        urls_path = "./util/Owner_URLs_{}.json".format(country)
        with open(urls_path, "r", encoding="utf-8") as f:
            urls = json.load(f)  # dict

        thread_list = []
        symbols = list(urls.keys())
        for symbol in symbols[:20]:

            if urls[symbol] != '':
                thread_list.append(GetMorningStarData(symbol=symbol, base_url=urls[symbol], country=country))

        print("Downloading", len(thread_list))

        for i, thread in enumerate(thread_list):
            thread.name = thread.symbol
            # time.sleep(2)
            if i % 5 == 0 and i != 0:
                time.sleep(10)
            if i % 10 == 0 and i != 0:
                time.sleep(30)
            if i % 100 == 0 and i != 0:
                time.sleep(60)
            print("=== {} start threading ===".format(thread.symbol))
            thread.start()
        for thread in thread_list:
            thread.join()
    print("# Download # finish at:", time.ctime())
