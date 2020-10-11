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
# fileName:site 
# project: WalterSim
# author: theo_hui
# e-mail:Theo_hui@163.com
# purpose: 每个site为一个类
# creatData:2020/10/11

import uuid
import time

class Site:
    ''' 一个地理site'''

    # 一个site包含1个server和多个client
    # 本例子中一个site就差不多等于一个server

    def __init__(self,siteID):
        self.siteID=siteID

        self.currentSeqNo=None # 当前的序列编号
        self.CommittedVTS=None # 提交的时间向量


        # 容器集合
        self.Containers=[]
        # History历史集合
        self.History={}
        # 正在运行事务集合
        self.Transactions=[]
        # 锁
        self.Locks={}
    ##################辅助函数####################
    def get_oid_preferred_sites_id(self,oid):
        ''' 获得对象的首选站点'''
        for c in self.Containers:
            if oid in c.keys():
                return c['oid']['prefID']
    def is_oid_locally_replicated(self,oid):
        return self.get_oid_preferred_sites_id(oid)==self.siteID

    ################# 执行事务 ###################
    def starTx(self,x):
        x['tid']=uuid.uuid4() # 分配一个id
        x['startVTS']=self.CommittedVTS
        return x
    def write(self,x,oid,data):
        x['updates'].append(['DATA',oid,data])
        return x
    def setAdd(self,x,setid,id):
        x['updates'].append(['SET_ADD',setid,id])
        return x
    def setDel(self,x,setid,id):
        x['updates'].append(['SET_DEL',setid,id])
        return x
    def read(self,x,oid):
        if self.is_oid_locally_replicated(oid):
            # 本地复制的
            # 1.返回x中反应oid的
            states=[]
            for s in x['updates']:
                if s[1]==oid:
                    states.append(s)
            # 2. 返回历史中最新的提交
            # TODO: 等待 startVTS明确
            history=self.History[oid]
            return states,history,None
        else:
            # 本地复制的
            # 1.返回x中反应oid的
            states=[]
            for s in x['updates']:
                if s[1]==oid:
                    states.append(s)
            # 2. 返回历史中最新的提交
            # TODO: 等待 startVTS明确
            history=self.History[oid]
            # 3. 返回其主站点中的历史
            # TODO: 等待 全局历史明确
            site_history=None
            return states,history,site_history
    def setRead(self,x,setid):
        return self.read(x,setid)
    ###################提交####################
    def unmodified(self,oid,VTS):
        #TODO: 先凭感觉写
        prefSiteID=self.get_oid_preferred_sites_id(oid)
        for x in self.Transactions:
            if x['startVTS']>VTS[prefSiteID]:
                for c in x['update']:
                    if oid==c[1] and ("DATA" in c[0] or "SET" in c[0]):
                        return False
        return True

    def update(self,updates,version):
        ''' 更新'''
        for update in updates:
            if update[1] not in self.History.keys():
                self.History['oid']=[]
            self.History['oid'].append([update[0],update[2],version])

    def commiTx(self,x):
        ''' 提交事务'''
        #写集合
        x['writeset']=[]
        for update in x['updates']:
            if 'DATA'==update[0]:
                x['writeset'].append({update[1]:update})
        #所有要写的都是本地的
        for oid in x['writeset'].keys():
            if self.get_oid_preferred_sites_id(oid)!=self.siteID:
                #TODO:插桩 慢提交
                return -1
        # 快提交
        #TODO:插桩 快提交
        return 1

    #========快速提交=======#
    def fastCommit(self,x,lock):
        #请求锁
        lock.acquire()
        for oid in x['writeset'].keys():
            if self.unmodified(oid,x['startVTS']) and self.Locks[oid]==False:
                continue
            else:
                lock.release()
                x['outcome'] = "ABORTED"
                return x['outcome']
        #锁释放
        lock.release()
        while self.CommittedVTS[self.siteID]<x['seqno']-1:
            time.sleep(1)
        self.CommittedVTS=x['seqno']
        x['outcome']="COMMITTED"
        # todo: 传播算法

        return x['outcome']
    #=======慢速提交========#
    def slowCommit(self,x,lock):
        # 需要慢速提交的sites集合
        sites=set()
        for oid in x['writeset'].keys():
            sites.add(self.get_oid_preferred_sites_id(oid))
        #TODO: 慢提交空缺
    #========传播==========#
    def



