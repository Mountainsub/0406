#!/usr/bin/env python
# coding: utf-8
import warnings
from tables import NaturalNameWarning
warnings.filterwarnings('ignore', category=NaturalNameWarning)
import asyncio
import pandas as pd
import numpy as np
import datetime

from concurrent import futures

import sys
sys.path.append("..")
from lib.ddeclient import DDEClient

import time


class LastNPerfTime:
    def __init__(self, n):
        """
        過去n回の実行時間の合計を尺取り法で記録する
        """
        self.n = n
        self.count = 0
        self.sum_time = 0
        self.times = np.zeros(n)
        
        
    def start(self):
        """
        実行時間の計測を開始する
        """
        self.start_time = time.perf_counter() # timeより正確
        
    def end(self):
        """
        実行時間の計測を終了する
        """
        dtime = time.perf_counter() - self.start_time
        idx = self.count % self.n # self.nが2^xならここをビット論理積にできる
        time_n_before = self.times[idx]
        self.times[idx] = dtime
        self.count += 1
        self.sum_time += (dtime - time_n_before)
        
    def get_sum_time(self):
        """
        過去n回の実行時間を合計した値を返す
        """
        return self.sum_time

class ClientHolder():
    def __init__(self, idx, codes, weights,hdffoldername = "./data/"):
        """
        RSSサーバーに接続し、継続的に複数の銘柄の株価を取得する
        
        Parameters
        ----------
        idx: int
        ClientHolderにつける番号
        番号がかぶると同じファイルに書き込むことになる
        
        codes: array_like
        RSSサーバーにリクエストを送る銘柄のコード番号を格納したリスト
        
        """
        hdffilename = hdffoldername + str(idx).zfill(3) + ".hdf5" # 文字列・数値をゼロパディング（ゼロ埋め）するzfill()
        
        self.idx = idx
        self.clients = {}
        self.activate = {}
        self.array=[]
        self.close_value = "現在値" #price_request_str
        self.codes = codes
        self.weights = weights
        self.Boolean = True
        self.codes_attrsafe = 'code_' + np.array(codes).astype('object') # pandasを使ってhdfを作るとき、数字から始まる列名にできない
        try:
            with pd.HDFStore(self.hdffilename) as store:
                self.df = store["check/"+str(idx)]
        except:
            self.df = pd.DataFrame.from_dict(data={i: [0] for i in codes}, orient="index",columns=["activate"])
            pass
        
        # RSSサーバーに接続し、127個のDDEClientを作る
        self.connect_all()
        
        # データ保存用のファイルを開く
        self.hdffilename = hdffilename
        self.store = pd.HDFStore(hdffilename)
        self.key_name = "classidx_" + str(self.idx) 
        
        self.firststep = True
        self.checkbox = {}
        
        
        
        
    def connect_all(self):
        """
        RSSサーバーに接続する
        """
        for code in self.codes:
            try:
                self.clients[code] = DDEClient("rss", code)
            except Exception as e:
                print(f"an error occurred at code: {code} while connecting server.")
                pass
            else:
                df = self.df
                #df.loc[code, "activate"] = 0
                #raise Exception(e)
        return
    
    

    
    
    def get_price(self, code):
        """
        1つの銘柄の株価を取得する
        """ 
        
        #_executor = ThreadPoolExecutor(1)   
        client = self.clients[code]
        t1 = time.time()
        #print(self.df["activate"].sum())
        """
        if float(self.df["activate"].sum()) > 16:
            print(code,"waiting...")
        """  
        if True:
            try:
                val = client.request("現在値").decode("sjis")         
            except:
                with open("shares.txt", "a",encoding="utf-8") as f:
                    f.write(client.request("銘柄名称").decode("sjis"))
                pass
            else:
                pass
        if val == "":
            with open("shares2.txt", "a",encoding="utf-8") as f:
                f.write(client.request("銘柄名称").decode("sjis")+ "\n")

        return val 
        

    def delete(self, pre_code):
               
        
        client = self.clients[pre_code]
        client.__del__()
        self.df.loc[pre_code,"activate"] = 0
        
        client.__init__("rss", pre_code)
        
       
    
    
    def get_prices(self):
        """
        複数の銘柄の株価を取得し、保存する
        """
        

        temps =[]
        prices = {}
        prices['time_start'] = np.datetime64(datetime.datetime.now())
            

        for i, code in enumerate(self.codes):     
            prices[self.codes_attrsafe[i]] = self.get_price(code)
            
            try:
                pre_code= temps[-1]
            except Exception:
                pass
            else:
                pass
            temps.append(code)
            
        
        prices['time_end'] = np.datetime64(datetime.datetime.now())
        self.save(prices)
        self.firststep = False
        return prices


    def save(self, data_dict):
        """
        取得した株価を保存する
        """
        self.store.append(self.key_name, pd.DataFrame([data_dict]))
        
    def stop_execute2():
        now = datetime.datetime.now()
        currently = np.datetime64(now)
        Y= np.datetime64(now, "Y")
        M = np.datetime64(now, "M")
        D= np.datetime64(now, "D")
        #m = np.datetime64(now, "m")
        
        h= np.datetime64(now, "h")
        
        m = np.datetime64(now, "m")
        if h.astype("float64") == 9 and m.astype("float64") > 20:
            sys.exit()    
        else:
            pass
    
    
    
    
    def get_prices_forever(self):
        """
        継続的に株価を取得して保存し続ける
        """
        
        
        
        while True:
            t1 = time.time()
            try:
                prices= self.get_prices()
            except KeyboardInterrupt:
                break
            except Exception as e:
                raise Exception(e)
            else:
                t2 = time.time()
                v = self.calc(prices)
                print(v)
                
                #s2 = s2
                """
                dict = {str(int(self.idx)-126)+"~"+str(int(self.idx)): v}
                series = pd.Series(dict)
                with pd.HDFStore(self.hdffilename) as store:
                    store.put("restore/"+str(self.idx), series)
                """
                self.stop_execute2()
                

    def calc(self,prices):
        checkbox = self.checkbox
        num = 0
        
        #prices=itertools.chain.from_iterable(prices)
        for i, code in enumerate(self.codes):
            #num += float(prices[i])* float(weights[i]) 
            val = prices[self.codes_attrsafe[i]]
            try:
                float(val)
            except Exception as e:
                checkbox[self.codes_attrsafe[i]] = self.codes_attrsafe[i]
                self.checkbox = checkbox
                continue
            num += float(val)* float(self.weights[i]) 
        return num
    def check(self):
        print("activate")
        with open("shares2.txt", "a", encoding="utf-8") as f:
            for i in self.checkbox.values():
                print(i)
                f.write(i)
        return


if __name__ == '__main__':
    idx = int(sys.argv[1])
    foldername = sys.argv[2]
    codes = sys.argv[3:]
    holder = ClientHolder(idx, codes, foldername)
    
    holder.get_prices_forever()