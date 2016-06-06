# -*- coding:utf-8 -*- 
"""
历史行情数据导入
"""
import tushare as ts
import pymongo
import json
import time

conn = pymongo.MongoClient('127.0.0.1', 27017)

#沪深300指数
docs = []
for k,v in json.loads(ts.get_h_data('000300', start='2005-12-01', end=None, index=True).to_json(orient='index')).items():
	v["_id"] = time.strftime('%Y-%m-%d', time.localtime(int(k[:10])))
	docs.append(v)
conn.quant.csi300.insert_many(docs)

#中证300指数
docs = []
for k,v in json.loads(ts.get_h_data('000905', start='2005-12-01', end=None, index=True).to_json(orient='index')).items():
	v["_id"] = time.strftime('%Y-%m-%d', time.localtime(int(k[:10])))
	docs.append(v)
conn.quant.csi500.insert_many(docs)

#国债指数
docs = []
for k,v in json.loads(ts.get_h_data('000012', start='2005-12-01', end=None, index=True).to_json(orient='index')).items():
	v["_id"] = time.strftime('%Y-%m-%d', time.localtime(int(k[:10])))
	docs.append(v)
conn.quant.gbi.insert_many(docs)