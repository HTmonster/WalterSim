#!/usr/bin/python3
# -*- coding:utf-8 -*-
#   ____    ____    ______________
#  |    |  |    |  |              |
#  |    |  |    |  |_____    _____|
#  |    |__|    |       |    |
#  |     __     |       |    |
#  |    |  |    |       |    |
#  |    |  |    |       |    |
#  |____|  |____|       |____|
#
# fileName:main 
# project: WalterSim
# author: theo_hui
# e-mail:Theo_hui@163.com
# purpose: {文件作用描述｝
# creatData:2020/10/11

from flask import Flask
from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.serving import run_simple

# 共3个site

app1 = Flask("site01")
app2 = Flask("site02")
app3 = Flask("site03")



if __name__ == '__main__':
   run_simple('127.0.0.1', 5000, app)