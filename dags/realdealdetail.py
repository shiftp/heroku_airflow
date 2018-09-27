#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 16:09:33 2018

@author: Shiftp
"""
from __future__ import print_function
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from pymongo import MongoClient
import urllib.parse
import datetime
from bs4 import BeautifulSoup
import requests
from linebot import (LineBotApi, WebhookHandler, exceptions)
from linebot.exceptions import (InvalidSignatureError)
from linebot.models import *
from os import environ

###############################################################################
#                         股票機器人 Airflow股價自動推波                       #
###############################################################################

#自己APP的token
token=os.environ['MYTOKEN']
#自己的ID
ID=os.environ['MYID']

########### dag所有參數，就放在這裡面 ###########
args = {
    'owner': 'shiftp', #這個dag的擁有者
    'start_date': airflow.utils.dates.days_ago(0) #開啟時，設定往前幾天開始執行
}

########### dag設定檔 ###########
dag = DAG(
    dag_id='realdealdetail', #dag的名稱
    default_args=args, #把上方的參數放進去
    schedule_interval='10 * * * * *') #多久執行一次

########## 推撥訊息 ##########
def line_pust(content):
    line_bot_api = LineBotApi(token)
    line_bot_api.push_message(ID, TextSendMessage(text=content))

########## 推撥訊息 ##########
def show_user_stock_realdealdetail():  
    url = 'https://tw.stock.yahoo.com/q/ts?s=2492'
    list_req = requests.get(url)
    soup = BeautifulSoup(list_req.content, "html.parser")
    souptable = soup.find_all('table')[7].tbody.find_all('tr')[1].find_all('td')
    realtime = souptable[0].text
    realbid = souptable[3].text
    realvolumn = souptable[5].text
   # 'realtime' : realtime, 'realbid' : realbid, 'realvolumn' : realvolumn
    line_pust('時間' + realtime + ',價格 '+ realbid + ',成交量' + realvolumn)
    
task = PythonOperator(
    task_id='2492', #設定dag小分支的名稱
    python_callable=show_user_stock_realdealdetail, #指定要執行的function
    dag=dag #把上方的設定檔案丟入主程式
    )
    

