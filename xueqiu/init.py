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
dateInit = '2005-01-01'
conn = pymongo.MongoClient('127.0.0.1', 27017)
db = conn.quantmax

today = datetime.date.today().strftime("%Y-%m-%d")

def getDatas(symbol, start = None):
    start = start if start is not None else dateStart
    docs = []
    for k,v in json.loads(ts.get_h_data(symbol, start=start, end=None, index=True).to_json(orient='index')).items():
        v["_id"] = time.strftime('%Y-%m-%d', time.localtime(int(k[:10])))
        v["weekday"] = datetime.datetime.strptime(v["_id"], "%Y-%m-%d").date().isoweekday()
        if dateStart != dateInit and dateStart == v["_id"]:
            continue
        docs.append(v)
    return docs

#中小300指数
dateLast = db.sme300.find_one(sort=[('_id', pymongo.DESCENDING)])
dateStart = dateLast['_id'] if dateLast is not None else dateInit
if dateStart == dateInit or dateStart != today:
    docs = getDatas('399008', '2010-03-22')
    if len(docs) > 0:
        db.sme300.insert_many(docs)

#沪深300指数
dateLast = db.csi300.find_one(sort=[('_id', pymongo.DESCENDING)])
dateStart = dateLast['_id'] if dateLast is not None else dateInit
if dateStart == dateInit or dateStart != today:
    docs = getDatas('000300')
    if len(docs) > 0:   
        db.csi300.insert_many(docs)

#中证500指数
dateLast = db.csi500.find_one(sort=[('_id', pymongo.DESCENDING)])
dateStart = dateLast['_id'] if dateLast is not None else dateInit
if dateStart != dateInit or dateStart != today:
    docs = getDatas('000905')
    if len(docs) > 0:
        db.csi500.insert_many(docs)

#国债指数
dateLast = db.gbi.find_one(sort=[('_id', pymongo.DESCENDING)])
dateStart = dateLast['_id'] if dateLast is not None else dateInit
if dateStart == dateInit or dateStart != today:
    docs = getDatas('000012')
    if len(docs) > 0:
        db.gbi.insert_many(docs)
