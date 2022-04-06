import pandas as pd
import numpy as np
import time 
import sys
import os
import random
import math
import datetime
from request.rakuten_rss import ind, rss , rss2 
from lib.ddeclient import DDEClient
from price_logger import ClientHolder 
from price_logger import LastNPerfTime
from multiprocessing import Process
def ind():
	indexes = pd.read_csv("TOPIX_weight_jp.csv")

	indexes["コード"] = pd.to_numeric(indexes["コード"], errors='coerce')


	indexes_code = indexes["コード"].astype(int)
	
	for i,j in enumerate(indexes_code):
		indexes_code[i] = str(j) + ".T"
	indexes_code = np.array(indexes_code)
	indexes_code = indexes_code.flatten()

	for i,j in indexes.iterrows():
		# % を除去
		indexes.at[i, "TOPIXに占める個別銘柄のウェイト"] = indexes.loc[i, "TOPIXに占める個別銘柄のウェイト"]
	return [indexes_code, indexes]


def code_s(k):
	array = []
	weights = []
	count = 0
	
	
	# 以下csvファイルを都合いいようにエディット

	inde = ind()
	indexes_code, indexes = inde[0], inde[1] 
	
	# ddeを取得、格納、ウエイトをかけて計算
	
	for i,j in enumerate(indexes_code, start = k): 
		count += 1
		
		c = indexes["コード"][i]
		w = indexes["TOPIXに占める個別銘柄のウェイト"][i]
		weights.append(w)
		array.append(str(int(c))+ ".T")
		
		if k == 2142 and count ==39:
			break

		if count >= 126:
			break

	return [array, weights]


def stop_execute():
	now = datetime.datetime.now()
	currently = np.datetime64(now)
	Y= np.datetime64(now, "Y")
	M = np.datetime64(now, "M")
	D= np.datetime64(now, "D")
	#m = np.datetime64(now, "m")
	h= np.datetime64(now, "h")
	m = np.datetime64(now, "m")
	hour = now.hour
	minute = now.minute
	if  hour >= 15:
		print("今日は閉場です。")
		sys.exit()
	if (hour == 11 and minute > 30 ) or (hour==12 and minute <30):
		print("お昼休みです。")
		temp = pd.datetime(2022, 4, 6, 12, 30)		
		temp = np.datetime64(temp)
		sleep_num = temp-currently-60
		tim = sleep_num.astype(int) / 10 ** 6
		t = str(tim)
		print(t)
		time.sleep(float(t))
	elif hour < 9:
		temp = pd.datetime(str(Y), str(M), str(D), 9, 0)		
		temp = np.datetime64(temp)
		sleep_num = float(temp.astype("float64")-currently.astype("float64")-60 )
		time.sleep(sleep_num)
	else:
		pass
		



if __name__ == '__main__':
    args = sys.argv # コマンドライン引数として開始地点のインデックスを数字で入力する
    
    count = 0
    
    idx = int(args[1]) * 126
    if len(args) > 2:
        switch = args[2]
        if switch == "on":
            stop_execute()
            #止める	
    holder = ClientHolder(idx, code_s(idx)[0], code_s(idx)[1])
    
    holder.get_prices_forever()
                      
