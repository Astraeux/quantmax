# -*- coding:utf-8 -*- 
"""
抓取历史行情数据
"""

import tushare as ts
import pymongo
import json
import time
import datetime

#初始日期
dateInit = '2005-12-01'
conn = pymongo.MongoClient('127.0.0.1', 27017)

today = datetime.date.today().strftime("%Y-%m-%d")

def getDatas(symbol):
	docs = []
	for k,v in json.loads(ts.get_h_data(symbol, start=dateStart, end=None, index=True).to_json(orient='index')).items():
		v["_id"] = time.strftime('%Y-%m-%d', time.localtime(int(k[:10])))
		if dateStart != dateInit and dateStart == v["_id"]:
			continue
		docs.append(v)
	return docs

#沪深300指数
dateLast = conn.quant.csi300.find_one(sort=[('_id', pymongo.DESCENDING)])
dateStart = dateLast['_id'] if dateLast is not None else dateInit
if dateStart != dateInit and dateStart != today:
	docs = getDatas('000300')
	if len(docs) > 0:
		conn.quant.csi300.insert_many(docs)


#中证300指数
dateLast = conn.quant.csi500.find_one(sort=[('_id', pymongo.DESCENDING)])
dateStart = dateLast['_id'] if dateLast is not None else dateInit
if dateStart != dateInit and dateStart != today:
	docs = getDatas('000905')
	if len(docs) > 0:
		conn.quant.csi500.insert_many(docs)

#国债指数
dateLast = conn.quant.gbi.find_one(sort=[('_id', pymongo.DESCENDING)])
dateStart = dateLast['_id'] if dateLast is not None else dateInit
if dateStart != dateInit and dateStart != today:
	docs = getDatas('000012')
	if len(docs) > 0:
		conn.quant.gbi.insert_many(docs)
