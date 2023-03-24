# 导入 akshare 和 pandas 库
import queue
import time
import akshare as ak
import pandas as pd
import threading

def get_stock_info(stock_code,q):
    try:

        stock_zh_a_hist_df,b=ak.stock_zh_a_hist(symbol=stock_code,period="weekly",adjust="hfq")#, start_date='20211010', end_date='20230322')
    except Exception as e:
        q.acquire()
        print(stock_code+" error to get")
        q.release()
        return 0
    if not "ST" in b and not stock_zh_a_hist_df.empty:
        # 计算均线
        ma5 = stock_zh_a_hist_df['收盘'].rolling(20).mean() # 20周均线
        ma10 = stock_zh_a_hist_df['收盘'].rolling(40).mean() # 40周均线
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

            if stock_zh_a_hist_df.iloc[-5:]['成交量'].mean()>2*mean_all['成交量'].mean():
                if stock_zh_a_hist_df.iloc[-5:]['收盘'].mean()<((mean_all['收盘'].mean()-min(mean_all['收盘']))/10+min(mean_all['收盘'])):
                    q.acquire()
                    print(b,len(mean_all['成交量']),stock_zh_a_hist_df.iloc[-5:]['成交量'].mean(),mean_all['成交量'].mean(),stock_zh_a_hist_df.iloc[-5:]['收盘'].mean(),min(mean_all['收盘']),max(mean_all['收盘']))
                    q.release()

# 获取所有 A 股股票代码
stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
# 只保留代码列
stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[['代码']]
# 遍历所有股票代码
threads = []
stock_zh_a_hist_df = pd.DataFrame()
stock_code_list=[]
q = threading.Lock()
print("股票名称","此周期长度","近五周成交量","此周期成交量","近五周价格","此周期最低价格","此周期最高价格")
for i in range(0, len(stock_zh_a_spot_em_df['代码']), 10):
    for stock_code in stock_zh_a_spot_em_df['代码'][i:i+10]:
        t = threading.Thread(target=get_stock_info, args=(stock_code, q))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    time.sleep(1)

