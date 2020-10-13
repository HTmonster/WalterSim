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
# purpose: site 0
# creatData:2020/10/12


from flask import Flask,request
import uuid
import requests,json
import threading
import time
import multiprocessing
from flask_cors import CORS

import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# 共2个site
app = Flask("server00")
siteID=0           #本site

#========================服务器列表信息====================#
servers=[
   #名称       id   是否本服务器    地址
   ["server00", 0,  True, "http://127.0.0.1:5000"],
   ["server01", 1,  False,"http://127.0.0.1:5001"]
]

#========================存储对象区======================#
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
object_locks_mutex=threading.Lock() #对象锁互斥锁
#========================公共变量区=======================#


currentSeqNo=0                    # 当前的序列编号
CommittedVTS = [0,0]              # 提交的时间向量
GotVTS       = [0,0]              # 接收站点时间向量

 # 历史记录
History = {
   # oid： [[ ['WRITE', oid,     data],        <site,seqno>],]
   "ssA01":[[['WRITE',"ssA01","Alice的说说1。"],[0,-1]],],
   "ssA02":[[['WRITE',"ssA02","Alice的说说2。"],[0,-1]],]
}                       

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
      his_list=[]
      for his in History[oid]:
         version  = his[1]
         site,seq = version
         if seq <=VTS[site]:
            his_list.append(his)
      return his_list
#--------------------对象锁操作相关-----------------------------#    
# 锁加锁
def prepare_lock(tid,oids,startVTS,retlist):
   ''' 投票准备并尝试加锁 '''
   object_locks_mutex.acquire()
   for oid in oids:
      #没有修改且没有加锁
      if unmodified(oid,startVTS) and oid not in object_locks.keys():
         continue
      else:
         retlist.append(False) #返回假
         break
   else:
      # 所有都满足 尝试加锁
      for oid in oids:
         object_locks[oid]=tid
      retlist.append(True)    # 返回真
   object_locks_mutex.release()

# 锁解锁
def abort_unlock(tid):
   ''' 投票取消解锁 '''
   object_locks_mutex.acquire()
   del_list=[]
   for key in object_locks.keys():
      if object_locks[key]==tid:
         del_list.append(key)
   for key in del_list:
      del object_locks[key]
   object_locks_mutex.release()


#-------------------------执行事务-----------------#
def starTx(x):
   x['tid'] = str(uuid.uuid4())     # 生成一个随机id
   x['startVTS'] = CommittedVTS[:]  # 开始时候的时间向量
   x['updates'] = []
   return x

#写一个对象
def write(x, oid, data):
   x['updates'].append(['WRITE', oid, data])
   return None

#集合加
def setAdd(x, setid, id):
   x['updates'].append(['SET_ADD', setid, id])
   return None

#集合减
def setDel(x, setid, id):
   x['updates'].append(['SET_DEL', setid, id])
   return None

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
      hiss=history_VTS_visible(oid,x['startVTS'])

      # 对于常规对象 walter 返回x.updates中的最新提交
      # 如果没有 返回 history中最后一次提交
      data=states[-1][2] if len(states)>0 else hiss[-1][0][2]
      return data
   else:
      # 非本地复制的
      # 1.返回x中反应oid的
      states = []
      for s in x['updates']:
         if s[1] == oid:
            states.append(s)
      # 2. 返回历史中最新的提交
      hiss=history_VTS_visible(oid,x['startVTS'])
      # 3. 返回其主站点中的历史
      prefID=get_oid_preferred_sites_id(oid)
      siteUrl=servers[prefID][3]
      # 远程请求
      res=requests.post(siteUrl+"/history",json={"oid":oid,"VTS":x['startVTS']})
      site_hiss=json.loads(res.text).get('data')

      # 对于常规对象 walter 返回x.updates中的最新提交
      # 如果没有 返回 history中最后一次提交
      if len(states)>0:
         data=states[-1][2]
      elif len(site_hiss)>0:
         data=site_hiss[-1][0][2]
      else:
         data=hiss[-1][0][2]
      return data

# 集合读
def setRead(x,setid):
  return read(x,setid)

#-----------------------事务提交---------------------------
def unmodified(oid,VTS):
   '''对象没被修改过'''
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
      History[oid].append([update,version])

def fastCommit(x):
   ''' 快提交  '''
   print("\n  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[线程] 快提交━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
   print("  ┃ ",x)
   global currentSeqNo

   #加锁
   ThreadLock.acquire()
   for oid in x['writeset'].keys():
      if unmodified(oid, x['startVTS']) and oid not in object_locks.keys():
         continue
      else:
         # 解锁
         ThreadLock.release()
         print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━× ABORTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")
         x['outcome'] = "ABORTED"
         return x['outcome']

   # 提交没有冲突 开始更新
   print("  ┃ ▶ 开始更新History     ",end="")
   currentSeqNo +=1
   x['seqno']=currentSeqNo
   update(x['updates'],(siteID,x['seqno']))
   print("  ┃ ☑ 更新结束")
   #解锁
   ThreadLock.release()
   #等待
   print("  ┃ ▶ 等待提交排序        ",end="")
   while CommittedVTS[siteID] < x['seqno'] - 1:
      time.sleep(2)
   CommittedVTS[siteID] = x['seqno']

   print("  ┃ ☑ 提交结束")
   x['outcome'] = "COMMITTED"
   print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━√ COMMITTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")

   #进行传播
   p=multiprocessing.Process(target=propagate,args=(x,))
   p.start()
   # p.join()
   return x['outcome']

def slowCommit(x):
   ''' 慢提交 '''
   print("\n  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[线程] 慢提交━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
   print("  ┃ ",x)
   global currentSeqNo

   #获得所有写入对象的首选站点(非本站点)
   sites={}
   for oid in x['writeset'].keys():
      pref_siteID=objects[oid]['prefID']
      if pref_siteID not in sites:
         sites[pref_siteID]=[]
      sites[pref_siteID].append(oid)
   print("  ┃   要写入的站点：",sites)
   #远程调用投票
   votes={}
   for site,oids in sites.items():
      serverUrl=servers[site][3]
      print("  ┃▲  请求投票 {} URL:{}".format(oids,serverUrl))
      if site!=siteID:
         res=requests.post(serverUrl+"/prepare",json={"tid":x['tid'],"oids":oids,"startVTS":x["startVTS"],"id":siteID})
         vote=json.loads(res.text).get("status")
      else:
         retLsit=[]
         prepare_lock(x['tid'],oids,x["startVTS"],retList)
         vote=retLsit[0]
      print("  ┃    ▷",vote)
      votes[site]=(True if vote=="YES" else False)
   print("  ┃   ★ 投票结果",votes)
   #如果所有的投票通过
   if all(list(votes.values())):
      ThreadLock.acquire()
      print("  ┃ ▶ 开始更新History    ",end="")
      currentSeqNo +=1
      x['seqno']=currentSeqNo
      update(x['updates'],(siteID,x['seqno']))
      print("  ┃ ☑ 更新结束")
      ThreadLock.release()
      print("  ┃ ▶ 等待提交排序        ",end="")
      while CommittedVTS[siteID] < x['seqno'] - 1:
         time.sleep(2)
      CommittedVTS[siteID] = x['seqno']
      print("  ┃ ☑ 等待结束")
      print("  ┃ ▶ 释放本地加锁        ",end="")
      abort_unlock(x["tid"])
      print("  ┃ ☑ 释放结束")
      x['outcome'] = "COMMITTED"
      print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━√ COMMITTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")

      #进行传播
      p=multiprocessing.Process(target=propagate,args=(x,))
      p.start()
      # p.join()
      return x['outcome']
   else:
      #投票失败 解锁
      print("  ┃ ▶ 解锁            ",end="")
      for site,vote in votes.items():
         if vote and site!=siteID:
            serverUrl=servers[site][3]
            res=requests.post(serverUrl+"/abort",json={"tid":x['tid'],"id":siteID})
      print("  ┃ ☑ 释放结束")
      print("  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━× ABORTED！━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")
      x['outcome'] = "ABORTED"
      return x['outcome']

def commiTx(x):
   ''' 提交事务'''
   #获得写集合
   x['writeset'] = {}
   for update in x['updates']:
      if 'WRITE' == update[0]:
         x['writeset'][update[1]]=update
   #如果所有要写的都是本地的
   print("[3.1] ---------获得写集合---------")
   print(x['writeset'])
   for oid in x['writeset'].keys():
      if get_oid_preferred_sites_id(oid) != siteID:
         print("[3.2] ---------慢提交--------------")
         com=threading.Thread(target=slowCommit,args=(x,))
         com.start()
         com.join()
         return 
   # 执行快提交
   print("[3.2] ---------快提交--------------")
   com=threading.Thread(target=fastCommit,args=(x,))
   com.start()
   com.join()

   print("==================================[事务 {}]======================================\n\n".format(x['outcome']))

   return 

#-------------------------同步传播--------------------------#

def propagate(x):
   # 给所有服务器传播同步信号
   print("╭────────────────────────────────────────────────[进程] 同步传播─────────────────────────────────────────────────────╮")
   print("│ ",x)
   print("│ ------------------------------------------------1. 传播----------------------------------------------------------")
   for server in servers:
      # 非本服务器
      if server[2]==False:
         serverUrl=server[3]
         print("│▲  同步到 站点 {} URL:{}".format(server[0],serverUrl))
         res=requests.post(serverUrl+"/propagate",json={"x":x,"id":siteID})
         print("|    ▷",res.text.replace("\n",""))
         #TODO:f+1 情况 和返回信息确认 先空缺
   # 标志
   print("| ♢事务现在 是 disaster-safe durable")
   x['mark']="disaster-safe durable"
   # 发送信号
   print("│ ------------------------------------------------2. 灾难安全备份------------------------------------------------------")
   for server in servers:
      # 非本服务器
      if not server[2]:
         serverUrl=server[3]
         print("│▲  同步到 站点 {} URL:{}".format(server[0],serverUrl))
         res=requests.post(serverUrl+"/ds_durable",json={"x":x,"id":siteID})
         print("|    ▷",res.text.replace("\n",""))
   # 标志
   print("| ♢事务现在 是 globally visible")
   x['mark'] = "globally visible"
   print("╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯")

#=======================http 接口区========================#

#刷新获得服务器的内容
@app.route("/flush")
def flush():
   myHistory,otherHistory={},{}
   for key,value in History.items():
      if is_oid_locally_replicated(key):
         myHistory[key]=value
      else:
         otherHistory[key]=value
   
   return {"myHistory":myHistory,"otherHistory":otherHistory,"currentSeqNo":currentSeqNo,"CommittedVTS":CommittedVTS,"GotVTS":GotVTS}




#通过本接口处理一个事务 事务内容通过POST进行提交
@app.route("/transaction",methods=['POST'])
def transcations():
   #0.生成一个事务
   print("\n\n==================================[事务]======================================")
   x={}
   #1.开始执行事务
   print("--------------------[1] 开始事务.--------------------")
   x=starTx(x)
   print(x)
   #2.执行事务中的具体动作
   print("--------------------[2] 操作对象.--------------------")
   event_list=request.get_json() #  获取事件的信息
   print(event_list)
   datas={}                      #  存储操作的结果数据
   for event in event_list:
      event_type=event[0] #动作名 也就是要执行的函数
      # 调用对应的函数直接运行
      ret=globals()[event_type](x,event[1],event[2]) if "read" not in event_type.lower() else globals()[event_type](x,event[1])
      if ret!=None:
         datas[event[1]]=ret
   print(x)
   #3.尝试提交事务
   print("-------------------[3] 尝试提交.---------------------")
   commiTx(x)
   return {"status":x['outcome'],"data":datas}

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
   print("\n╭──────────────────────────────────────────────────────────────────────────╮")
   print("│=> 接受到来自站点{}传播请求".format(j))
   print("│   事务:",x)
   for i in range(len(x['startVTS'])):
      if x['startVTS'][i]>GotVTS[i]:
         print("│   × 条件1不满足",x['startVTS'],GotVTS)
         print("│<= {\"status\":\"ERROR\"}")
         print("╰──────────────────────────────────────────────────────────────────────────╯")
         return {"status":"ERROR"},200
   if GotVTS[j]!=x['seqno']-1:
      print("│   × 条件2不满足",x['startVTS'],GotVTS)
      print("│<= {\"status\":\"ERROR\"}")
      print("╰──────────────────────────────────────────────────────────────────────────╯")
      return {"status":"ERROR"},200
   
   update(x['updates'],(j,x['seqno']))
   GotVTS[j]=x['seqno']
   print("│   ☑ 更新完成")
   print("│         ♢GotVTS: ",GotVTS)
   print("│         ♢History: ",History)
   # 返回确认信息
   print("│<= {\"status\":\"OK\"}")
   print("╰──────────────────────────────────────────────────────────────────────────╯")
   return {"status":"OK","tid":x['tid']},200

#ds_durable接收接口
@app.route("/ds_durable",methods=['POST'])
def do_ds_durable():
   data=request.get_json()
   x=data['x']
   j=data['id']
   print("\n╭──────────────────────────────────────────────────────────────────────────╮")
   print("│=> 接受到来自站点{} 容灾请求".format(j))
   print("│   事务:",x)
   for i in range(len(x['startVTS'])):
      if x['startVTS'][i]>CommittedVTS[i]:
         print("│   × 条件1不满足",x['startVTS'],GotVTS)
         print("│<= {\"status\":\"ERROR\"}")
         print("╰──────────────────────────────────────────────────────────────────────────╯")
         return {"status":"ERROR"},200
   if CommittedVTS[j]!=x['seqno']-1:
      print("│   × 条件2不满足",x['startVTS'],GotVTS)
      print("│<= {\"status\":\"ERROR\"}")
      print("╰──────────────────────────────────────────────────────────────────────────╯")
      return {"status":"ERROR"},200
   #提交时间向量更新
   CommittedVTS[j]=x['seqno']
   print("│   ☑ 更新完成")
   print("│         ♢CommittedVTS: ",CommittedVTS)

   thread=threading.Thread(target=abort_unlock,args=(x['tid'],))
   thread.start()
   thread.join()
   print("│   ☑ 释放锁完成")
   # 返回确认信息
   print("│<= {\"status\":\"OK\"}")
   print("╰──────────────────────────────────────────────────────────────────────────╯")

   # 返回确认信息
   return {"status":"OK","tid":x['tid']},200

#慢投票接口
@app.route("/prepare",methods=['POST'])
def do_prepare():
   data=request.get_json()
   tid     =data['tid']
   oids    =data['oids']
   startVTS=data['startVTS']
   j       =data['id']
   print("\n╭──────────────────────────────────────────────────────────────────────────╮")
   print("│=> 接受到来自站点{} 慢提交投票请求".format(j))
   print("│   tid:{} \t oids:{}\t startVTS:{}".format(tid,oids,startVTS))

   # 判断并尝试开锁
   retList=[]
   thread=threading.Thread(target=prepare_lock,args=(tid,oids,startVTS,retList))
   thread.start()
   thread.join()
   ret=retList[0]
   print("│▶ 投票结果 ",ret)
   print("│<= \"status\":\"{}\"".format("YES" if ret else "NO"))
   print("╰──────────────────────────────────────────────────────────────────────────╯")
   
   return {"status":"YES" if ret else "NO"}
   
# 慢投票解锁  
@app.route("/abort",methods=['POST'])
def do_abort():
   data=request.get_json()
   tid     =data['tid']
   j       =data['id']
   print("\n╭──────────────────────────────────────────────────────────────────────────╮")
   print("│=> 接受到来自站点{} 慢提交投票解锁请求".format(j))  

   thread=threading.Thread(target=abort_unlock,args=(tid,))
   thread.start()
   thread.join()
   
   print("│▶ 完成")
   print("╰──────────────────────────────────────────────────────────────────────────╯")
   return {"status":"OK"}

if __name__ == '__main__':
   CORS(app, supports_credentials=True)
   app.run("127.0.0.1",5000,debug=True)