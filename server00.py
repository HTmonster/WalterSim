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
# fileName:site1 
# project: WalterSim
# author: theo_hui
# e-mail:Theo_hui@163.com
# purpose: site 1
# creatData:2020/10/12


from flask import Flask,request
import uuid
import requests,json
import threading
import time
import multiprocessing


# 共3个site
app = Flask("server00")


#========================服务器存储对象区======================#
objects={
   #oid: 内容
   #本站点首先
   "ssA01":{
      "prefID":0,#首站点id
      "data":"Alice的说说1。"
   },
   "ssA02":{
      "prefID":0,#首站点id
      "data":"Alice的说说2。"
   },

   #非本站点首先
   "ssB01":{
      "prefID":1,#首站点id
      "data":"Bob的说说1。"
   }
}

#对象的锁
object_locks={}


#========================服务器列表信息====================#
servers=[
   #名称       id   是否本服务器    地址
   ["server00", 0,  True, "http://127.0.0.0.1:5000"],
   ["server01", 1,  False,"http://127.0.0.0.1:5001"]
]

#========================公共变量区=======================#
siteID=0           #本site

currentSeqNo=0                    # 当前的序列编号
CommittedVTS = [0,0]     # 提交的时间向量
GotVTS       = [0,0]     # 接收站点时间向量


History = {}                        # 历史记录

currentTrans=[]
ThreadLock=threading.Lock()         # 多线程锁

#---------------辅助函数--------------------
def get_oid_preferred_sites_id(oid):
   ''' 获得对象的首选站点'''
   if oid in objects.keys():
      return objects[oid]['prefID']
   else:
      return None

# 判断是不是本地复制
def is_oid_locally_replicated(oid):
   return get_oid_preferred_sites_id(oid) == siteID


# 获得本地history中对于VTS中可见的部分
def history_VTS_visible(oid,VTS):
   if oid not in History.keys():
      return []
   else:
      versions=[]
      for his in History[oid]:
         seq      = his[1][1]
         visiable = his[2]
         if seq <VTS[siteID] and visiable:
            versions.append(his[1])
      return versions

#-------------------------执行事务-----------------#
def starTx(x):
   x['tid'] = str(uuid.uuid4())     # 生成一个随机id
   x['startVTS'] = CommittedVTS[:]  # 开始时候的时间向量
   x['updates'] = []
   return x

#写一个对象
def write(x, oid, data):
   x['updates'].append(['WRITE', oid, data])
   return x

#集合加
def setAdd(x, setid, id):
   x['updates'].append(['SET_ADD', setid, id])
   return x

#集合减
def setDel(x, setid, id):
   x['updates'].append(['SET_DEL', setid, id])
   return x

#读取一个对象
def read(x, oid):
   if is_oid_locally_replicated(oid):
      # 本地复制的
      # 1.返回x中反应oid的
      states = []
      for s in x['updates']:
         if s[1] == oid:
            states.append(s)
      # 2. 返回历史中最新的提交
      versions=history_VTS_visible(oid,x['startVTS'])
      return states, versions
   else:
      # 非本地复制的
      # 1.返回x中反应oid的
      states = []
      for s in x['updates']:
         if s[1] == oid:
            states.append(s)
      # 2. 返回历史中最新的提交
      versions=history_VTS_visible(oid,x['startVTS'])
      # 3. 返回其主站点中的历史
      prefID=get_oid_preferred_sites_id(oid)
      siteUrl=servers[prefID][3]
      # 远程请求
      res=requests.post(siteUrl+"/history/",data={"oid":oid,"VTS":x['startVTS']})
      site_versions=json.loads(res.text).get('data')
      return states, versions,site_versions

# 集合读
def setRead(x,setid):
  return read(x,setid)

#-----------------------事务提交---------------------------

# 对象没被修改过
def unmodified(oid,VTS):
   if oid not in History.keys():
      return True
   else:
      for his in History[oid]:
         seq=his[1][1]
         if VTS[siteID]<seq:
            return False
      return True

def update(updates,version):
  ''' 更新'''
  for update in updates:
      oid=update[1]
      if oid not in History.keys():
          History[oid]=[]
      History[oid].append([update,version,False])

def fastCommit(x):
   #加锁
   global currentSeqNo
   ThreadLock.acquire()
   for oid in x['writeset'].keys():
      if unmodified(oid, x['startVTS']) and oid not in object_locks.keys():
         continue
      else:
         ThreadLock.release()
         x['outcome'] = "ABORTED"
         return x['outcome']

   currentSeqNo +=1
   x['seqno']=currentSeqNo
   print(x['seqno'])

   update(x['updates'],(siteID,x['seqno']))
   print(History)
   #解锁
   ThreadLock.release()

   #等待
   while CommittedVTS[siteID] < x['seqno'] - 1:
      time.sleep(2)
   CommittedVTS[siteID] = x['seqno']

   x['outcome'] = "COMMITTED"
   print(x['outcome'])

   #进行传播
   print("传播")
   print(x)
   p=multiprocessing.Process(target=propagate,args=(x,))
   p.start()
   p.join()

   return x['outcome']







def commiTx(x):
   ''' 提交事务'''
   #获得写集合
   x['writeset'] = {}
   for update in x['updates']:
      if 'WRITE' == update[0]:
         x['writeset'][update[1]]=update
   #如果所有要写的都是本地的
   print(x['writeset'])
   for oid in x['writeset'].keys():
      if get_oid_preferred_sites_id(oid) != siteID:
         # TODO：执行慢提交
         return -1
   # 执行快提交
   print(x)
   com=threading.Thread(target=fastCommit,args=(x,))
   com.start()

   return -1

#-------------------------同步传播--------------------------#

def propagate(x):
   # 给所有服务器传播同步信号
   for server in servers:
      # 非本服务器
      if server[2]==False:
         serverUrl=server[3]
         res=requests.post("http://127.0.0.1:5001/propagate",json={"x":x,"id":siteID})
         print(res.text)
           #TODO:f+1 情况 和返回信息确认 先空缺
   # 标志
   x['mark']="disaster-safe durable"
   # 发送信号
   for server in servers:
      # 非本服务器
      if not server[2]:
         serverUrl=server[3]
         print(serverUrl)
         res=requests.post("http://127.0.0.1:5001/ds_durable",json={"x":x,"id":siteID})
         print(res.text)
           #TODO:f+1 情况 和返回信息确认 先空缺
   # 标志
   x['mark'] = "globally visible"

#=======================http 接口区========================#

#通过本接口处理一个事务 事务内容通过POST进行提交
@app.route("/transaction",methods=['POST'])
def transcations():
   #0.生成一个事务
   x={}
   #1.开始执行事务
   x=starTx(x)
   print(x)
   #2.执行事务中的具体动作
   event_list=request.get_json() #  获取事件的信息
   #例子
   #data=[
   #    ["write","ssA01","Alice修改后的说说01"],
   #
   # ]
   for event in event_list:
      event_type=event[0] #动作名 也就是要执行的函数
      # 调用对应的函数直接运行
      globals()[event_type](x,event[1],event[2]) if "read" not in event_type.lower() else globals()[event_type](x,event[1])
   #3.尝试提交事务
   print(x)
   commiTx(x)
   return "yes"


#history接口
@app.route("/history",methods=['POST'])
def history():
   data=request.get_json()
   return {"data":history_VTS_visible(data["oid"],data["VTS"])}

#同步传播接收接口
@app.route("/propagate",methods=['POST'])
def do_propagate():
   data=request.get_json()
   x=data['x']
   j=data['id']
   print("recive propagate from {}:\n{}".format(j,x))
   for i in range(len(x['startVTS'])):
      if x['startVTS'][i]>GotVTS[i]:
         return {"status":"ERROR"},200
   if GotVTS[j]!=x['seqno']-1:
      return {"status":"ERROR"},200
   update(x['updates'],(siteID,x['seqno']))
   GotVTS[j]=x['seqno']
   # 返回确认信息
   return {"status":"OK","tid":x['tid']},200

#ds_durable接收接口
@app.route("/ds_durable",methods=['POST'])
def do_ds_durable():
   data=request.get_json()
   x=data['x']
   j=data['id']
   print("recive ds_durable from {}:\n{}".format(j,x))
   for i in range(len(x['startVTS'])):
      if x['startVTS'][i]>CommittedVTS[i]:
         return {"status":"ERROR"},200
   if CommittedVTS[j]!=x['seqno']-1:
      return {"status":"ERROR"},200
   #提交时间向量更新
   CommittedVTS[j]=x['seqno']
   # TODO:释放所有的对象锁

   # 返回确认信息
   return {"status":"OK","tid":x['tid']},200



if __name__ == '__main__':
   app.run("127.0.0.1",5000,debug=True)