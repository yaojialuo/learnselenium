import requests
import json
import pandas as pd
import datetime
import os
import time
# %load_ext autoreload
# %autoreload 2
#http://wenda.tdx.com.cn/site/wenda/index.html
#http://excalc.icfqs.com:7616/site/tdx-pc-find/page_xskx.html
td=datetime.datetime.today()
s=requests.session()
url= 'http://excalc.icfqs.com:7616/TQLEX'
cookies = {
    'ASPSessionID': '',
}

headers = {
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Content-Type': 'text/plain',
    'Accept': '*/*',
    'Origin': 'http://excalc.icfqs.com:7616',
    'Referer': 'http://excalc.icfqs.com:7616/site/tdx-zbfx/page-ddlj.html',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
}

params = (
    ('Entry', 'HQServ.hq_nlp'),
)

def reqdata(funcId,btime,etime,code,offset,count=200):

    dic={"modname":"TickAly"}
    dic["count"]=count
    dic["funcId"]=funcId

    if btime is None:
        time="time,11700,33600"
    else:
        btime=11700+btime
        etime=11700+etime
        time=f"time,{btime},{etime}"
    cond = time
    if code is not None:
        if code[0]=="6":
            code="1"+code
        else:
            code="0"+code
        code="code,"+code
        cond=time+"|"+code
    dic['cond']=cond
    dic['offset']=offset
    data=json.dumps([dic])
    print(data)
    response = s.post(url, headers=headers, params=params, cookies=cookies,
                             data=data, verify=False)
    return json.loads(response.text)['datas']


#funcid==8
type_dic={1: "等量委托", 2: "等额委托", 3: "等量成交", 4: "等额成交"}
#dir 1:buy
def parseMarketCxd(datas):
    rows=[]

    for row in datas:
        wtbh=row[0]
        btime = (datetime.datetime(td.year, td.month, td.day) + datetime.timedelta(hours=6, seconds=row[1])).strftime("%H:%M:%S")
        etime = (datetime.datetime(td.year, td.month, td.day) + datetime.timedelta(hours=6, seconds=row[2])).strftime("%H:%M:%S")
        market = row[3]
        code = row[4]
        name = row[5]
        type = type_dic[row[6]]
        sb = row[7]
        price = row[8]/10000
        unit = row[9]
        vol = row[10]
        times = row[11]
        total = row[12]
        rows.append([wtbh,code,name,btime,etime,type,sb,price,unit,vol,times,total])
    return pd.DataFrame(rows,columns=['wtbh','code','name','btime','etime','type','sb','price','total_amount','vol','times','total'])
# funcid==2
# {0: "超大单主卖", 1: "超大单主买", 2: "大单主卖", 3: "大单主买", 4: "中单卖", 5: "中单买"}
# vol是挂单手数,total_vol 成交手数,vol-total_vol 是撤单
# eat_dws 是吃单数
# total_amount 是成交金额
# zd_vol 吃单金额
# zd_amount 被动成交手数
# bd_vol 被动成交金额 = total_amount-zd_amount
# bd_amount 成交价格数  与 委托价格数 的差异数
def parseMarketHq(datas):
    # print('zljk:{0: "超大单主卖", 1: "超大单主买", 2: "大单主卖", 3: "大单主买", 4: "中单卖", 5: "中单买"}')
    zljk_dic={0: "超大单主卖", 1: "超大单主买", 2: "大单主卖", 3: "大单主买", 4: "中单卖", 5: "中单买"}
    rows = []

    for row in datas:
        name = row[0]
        market=row[1]
        code=row[2]
        no=row[3]
        time = (datetime.datetime(td.year, td.month, td.day) + datetime.timedelta(hours=6, seconds=row[4])).strftime("%H:%M:%S")
        price= row[5]/10000
        vol=row[6]
        amount=row[7]
        zljk=zljk_dic[row[8]]
        total_vol=row[9]
        total_amount=row[10]
        eat_dws=row[11]
        zd_vol=row[12]
        zd_amount=row[13]
        bd_vol=row[14]
        bd_amount=row[15]
        rows.append([no,code,name,time,price,vol,amount,zljk,total_vol,total_amount,eat_dws,zd_vol,zd_amount,bd_vol,bd_amount])
    return pd.DataFrame(rows,
        columns=['no','code','name','time','price','vol','amount','zljk','total_vol','total_amount','eat_dws','zd_vol','zd_amount','bd_vol','bd_amount'])



def getTodayMarketHq(path,btime=None,etime=None,code=None,offset=0,mode='a'):
    now_time = datetime.datetime.now()
    today = str(now_time.date())
    path=os.path.join(path,today+f"c{code}b{btime}e{etime}.csv")
    while True:
    #for i in range(10):
        time.sleep(0.1)
        print("getTodayMarketHq_try:",offset)
        try:
            datas = reqdata(2, btime,etime, code, offset)
        except Exception as e:
            print(e)
            time.sleep(1)
            continue
        df = parseMarketHq(datas)
        df.to_csv(path, header=False, index=False, mode=mode)
        if len(df)<200:
            break
        offset+=200

def getTodayMarketCxd(path,btime=None,etime=None,code=None,offset=0,mode='a'):
    now_time = datetime.datetime.now()
    today = str(now_time.date())
    path=os.path.join(path,today+f"c{code}b{btime}e{etime}.csv")
    print(path)
    while True:
    # for i in range(10):
        time.sleep(0.1)
        print("getTodayMarketCxd:",offset)
        try:
            datas = reqdata(8, btime,etime, code, offset)
        except Exception as e:
            time.sleep(1)
            continue
        df = parseMarketCxd(datas)
        df.to_csv(path, header=False, index=False, mode=mode)
        if len(df)<200:
            break
        offset+=200