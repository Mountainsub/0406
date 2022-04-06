
import pandas as pd
import numpy as np
import time 
import sys
import os
import ctypes
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import random
import math
import datetime

from lib.ddeclient import DDEClient
from price_logger import ClientHolder 
from price_logger import LastNPerfTime


class tameru:
    def __init__(self):
        self.hdffilename = "./data/sum.hdf5"
        self.store = pd.HDFStore(self.hdffilename)
        self.key_name = "testcase" 
        self.key_name2 = "timecase"

    def hozon(self, data_dict):
        #print("OK")
        self.store.append(self.key_name, data_dict)

    def hozon2(self, data_dict):
        #print("OK")
        self.store.append(self.key_name2, data_dict)
      
class up_or_down:
    def __init__(self, dif, dif2, switch):
        
        
        self.RED = '\033[31m'
        self.BLUE = '\033[34m'
        self.END = '\033[0m'
        
        self.switch = switch 
        if dif - dif2 >= 0.1:
            self.Boolean = "up"
        elif dif - dif2 <= 0.1:
            self.Boolean = "down"
        else:
            self.Boolean = "None"
    def judge(self):
        t = self.Boolean
        RED = self.RED
        BLUE = self.BLUE
        END = self.END
        if t == "up":
            string = RED +"計算した値が、実際のTOPIXよりaccceler-upです。"+END
            if self.switch == "down":
                print("買い時です！")
                self.switch == "up"
        elif t == "down":
            string = BLUE+"計算した値が、実際のTOPIXよりacceler-downです。"+END
            if self.switch == "up":
                print("売り時です！")
                self.switch == "down"
        else:
            string = "変化が小さいので手を加えない方がよいでしょう。"
        return string

    def lever(self):
        return self.switch



if __name__ == "__main__":
    ENABLE_PROCESSED_OUTPUT = 0x0001
    ENABLE_WRAP_AT_EOL_OUTPUT = 0x0002
    ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
    MODE = ENABLE_PROCESSED_OUTPUT + ENABLE_WRAP_AT_EOL_OUTPUT + ENABLE_VIRTUAL_TERMINAL_PROCESSING
    
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.GetStdHandle(-11)
    kernel32.SetConsoleMode(handle, MODE)

    t1 = time.time()
    calc = 0
    timer = LastNPerfTime(2**20)
    object_pass = "value"
    dde = DDEClient("rss", "TOPX")
    store_x = pd.HDFStore("./data/sum.hdf5")
    pre_topix = dde.request("現在値")
    pre_calc = 1919.3870320461579        
    switch = "neutral"
    while True:
        holder = tameru()
        
        timer.start()
        calc = 0
        x = 1
        
        for i in range(18):
            idx = i *126
            filename = "./data/" + str(idx).zfill(3)+ ".hdf5"
             
            try:
                with pd.HDFStore(filename) as store:
                    temp =store.get(object_pass)
            except:
                pass
            else:
                end = temp.tail(1)
                v = float(end["0"])
                while v==0:
                    now = datetime.datetime.now()
                    print(i, "attention", now)
                    with pd.HDFStore("./data/caution.hdf5") as store:
                        store.append("zero",pd.DataFrame({"caution":[now], "id": [i]})) 
                    x += 1
                    v = float(temp.iat[-1* x,0])
                else:
                    print(i, "pass")
                    x = 1
                calc += v
            
        
        dict = {"total": [calc]}
        timer.end()
                
        series = pd.DataFrame(dict)

        holder.hozon(series)
        temp = timer.get_sum_time()    
        dict = {"time": [temp]}
        df = pd.DataFrame(dict)
        holder.hozon2(df)   
        timer.count_one()
        now = datetime.datetime.now()
        topix_1976 = 434.3714486949468
        calc /= topix_1976*0.01
        dif = calc - pre_calc 
        topix = dde.request("現在値")
        dif2 = topix - pre_topix
        
        instance = up_or_down(dif, dif2, switch)
        
        if calc  < float(dde.request("現在値"))-10 or calc  > float(dde.request("現在値"))+10:
            print(calc,"取得結果がズレすぎています。")
            continue
        """
        elif calc  > float(topix):
            bool = "up"
            #up_or_down = RED + bool + END
        elif calc  < float(topix):
            bool = "down"
            #up_or_down = BLUE + bool + END
        else:
            bool = "equal" 
            #up_or_down = bool   
        """
        string = up_or_down.judge()
        print("取得時刻:"+str(now),"計算値:" + str(calc), string)
        store_x.append("consequence",pd.DataFrame({"time":[now], "calc":[calc], "up_or_down":bool}))
        pre_calc = calc
        pre_topix = topix
        switch = up_or_down.lever()