# 导入 akshare 和 pandas 库
import queue
import time
import akshare as ak
import pandas as pd
import threading
import smtplib
from email.mime.text import MIMEText
from email.header import Header

def get_stock_info(stock_code,q,p,coefficient,seq,last_trading_day):
    try:
        stock_zh_a_hist_df,b=ak.stock_zh_a_hist(symbol=stock_code,period="daily",adjust="hfq")#, start_date='20211010', end_date='20230322')
    except Exception as e:
        q.acquire()
        print(stock_code+str(e)+" error to get")
        q.release()
        seq.release()
        return 0
    

    if not "ST" in b and not stock_zh_a_hist_df.empty and stock_zh_a_hist_df.iloc[-1]["日期"]==last_trading_day:
        # 计算均线
        ma5 = stock_zh_a_hist_df['收盘'].rolling(20).mean() # 20周均线
        ma10 = stock_zh_a_hist_df['收盘'].rolling(60).mean() # 40周均线
        ma20 = stock_zh_a_hist_df['收盘'].rolling(100).mean() # 100周均线
        # 计算均线差分
        ma5_diff = ma5.diff()
        ma10_diff = ma10.diff()
        ma20_diff = ma20.diff()
        ma_diff = pd.concat([ma5_diff, ma10_diff, ma20_diff], axis=1)
        ma_diff.columns = ['ma5_diff', 'ma10_diff', 'ma20_diff']
        ma_diff = ma_diff.dropna()
        # 计算均线差分符号
        ma_diff_sign = ma_diff.applymap(lambda x: 1 if x > 0 else -1)
        # 计算均线差分符号和
        ma_diff_sign_sum = ma_diff_sign.sum(axis=1)
        # 计算均线差分符号和差分
        ma_diff_sign_sum_diff = ma_diff_sign_sum.diff()
        # 计算均线差分符号和差分符号
        ma_diff_sign_sum_diff_sign = ma_diff_sign_sum_diff.apply(lambda x: 1 if x > 0 else -1)
        # 计算均线差分符号和差分符号移位
        ma_diff_sign_sum_diff_sign_shift = ma_diff_sign_sum_diff_sign.shift(1)
        try:
            ma_diff_sign_sum_diff_sign_shift.iloc[0] = -1
        except Exception as e:
            return
        # 计算均线差分符号和差分符号移位差分
        ma_diff_sign_sum_diff_sign_shift_diff = ma_diff_sign_sum_diff_sign - ma_diff_sign_sum_diff_sign_shift
        ma_diff_sign_sum_diff_sign_shift_diff.iloc[0] = -2
        # 计算均线差分符号和差分符号移位差分的拐点
        ttt = ma_diff_sign_sum_diff_sign_shift_diff[ma_diff_sign_sum_diff_sign_shift_diff == 2].index
        # 判断是否存在拐点
        if len(ttt) > 2:
            # 获取最后一个拐点
            last_ma_diff_sign_sum_diff_sign_shift_diff_index = ttt[-1]
            mean_all=stock_zh_a_hist_df.iloc[last_ma_diff_sign_sum_diff_sign_shift_diff_index:]
            if len(mean_all['成交量'])<=5:
                last_ma_diff_sign_sum_diff_sign_shift_diff_index = ttt[-2]
            last_ma_diff_sign_sum_diff_sign_shift_diff_index = last_ma_diff_sign_sum_diff_sign_shift_diff_index - 1
            mean_all=stock_zh_a_hist_df.iloc[last_ma_diff_sign_sum_diff_sign_shift_diff_index:]
            # 判断成交量是否大于等于5周的平均成交量的两倍
            while coefficient>=1:
                if stock_zh_a_hist_df.iloc[-1:]['成交量'].mean()>coefficient*mean_all['成交量'].mean() and stock_zh_a_hist_df.iloc[-1:]['成交量'].mean()>stock_zh_a_hist_df.iloc[-5:]['成交量'].mean():
                    if stock_zh_a_hist_df.iloc[-5:]['收盘'].mean()<((mean_all['收盘'].mean()-min(mean_all['收盘']))/10+min(mean_all['收盘'])):
                        p.put(b+str(len(mean_all['成交量']))+"\t"+str(stock_zh_a_hist_df.iloc[-5:]['成交量'].mean())+"\t"+str(mean_all['成交量'].mean())+"\t"+str(stock_zh_a_hist_df.iloc[-5:]['收盘'].mean())+"\t"+str(min(mean_all['收盘']))+"\t"+str(max(mean_all['收盘']))+"\t"+str(coefficient)+"\n")
                        break    
                coefficient-=1
    seq.release()
# 发送邮件函数
def send_email(content):
    info=None
    with open("password.txt","r") as f:
        info=f.readlines()
    
    # 发件人邮箱
    sender = info[0].strip()
    # 收件人邮箱
    receivers = [sender]
    # 邮件主题
    subject = '股票策略结果'
    # 邮件正文
    message = MIMEText(content, 'plain', 'utf-8')
    message['From'] = Header("股票策略", 'utf-8')
    message['To'] = Header("收件人", 'utf-8')
    message['Subject'] = Header(subject, 'utf-8')
    # 发送邮件
    try:
        smtpObj = smtplib.SMTP_SSL('smtp.qq.com',465)
        smtpObj.ehlo()
        smtpObj.login(sender, info[1])
        smtpObj.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException as e:
        print("Error: 无法发送邮件 "+str(e))
def get_stock(coefficient):
    # 获取A股上一交易日日期
    x=ak.stock_zh_a_daily(symbol="sh000001")
    last_trading_day = x.iloc[-1]["date"].strftime("%Y-%m-%d")
    # 获取所有 A 股股票代码
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    # 只保留代码列
    stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[['代码']]
    # 筛选掉北交所、创业板、科创板、面临退市和已退市股票
    stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[stock_zh_a_spot_em_df['代码'].str.startswith(("600","601","603","605","000","002","003"))]

    # 遍历所有股票代码
    threads = []
    # stock_zh_a_hist_df = pd.DataFrame()
    # stock_code_list=[]
    q = threading.Lock()
    seq=threading.Semaphore(50)
    p=queue.Queue()
    content = "股票名称,此周期长度,近五日成交量,此周期成交量,近五日价格,此周期最低价格,此周期最高价格,当前系数\n"
    for stock_code in stock_zh_a_spot_em_df['代码']:
        t = threading.Thread(target=get_stock_info, args=(stock_code, q,p,coefficient,seq,last_trading_day))
        seq.acquire()
        t.start()
        while seq._value<=5:
            time.sleep(1)
    while not p.empty():
        content+=p.get()
    return content
coefficient=4
content=get_stock(coefficient)
if len(content.split("\n"))>2 :
    send_email(content)


