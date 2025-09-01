import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from tqdm import tqdm # type: ignore
import MetaTrader5 as mt5
from datetime import datetime, timedelta
import pandas as pd

factors_list = ['因子_開盤後15分鐘']
returns_list = ["回報_開盤15分鐘後5分鐘", "回報_開盤15分鐘15分鐘"]


def calc_ratio(x, type_=""):
    try:
        if type_ == "1":
            return x.iloc[0, 1] / x.iloc[0, 0] - 1  
        elif type_ == "2":
            return x.iloc[1, 1] / x.iloc[0, 0] - 1  
        else:
            return x.iloc[1, 1] / x.iloc[0, 0] - 1  
    except IndexError:
        return None  
    
def get_factor(df, d_early, d_late, factor_name):
    return (
    df[df['time'].isin([d_early, d_late])]
    .groupby("da", group_keys=False)[["op", "cl"]]  
    .apply(lambda x: calc_ratio(x, factor_name), include_groups=False)
    )

def df_factor_profit_bt(df, factors_list:list, returns_list:list, plots=True):
    ic_ratio_list = {}
    if len(factors_list) == 0: return; 
    if len(returns_list) == 0: return;
    
    for factor in factors_list:
        df['signal_dto'] = df[factor].apply(lambda x: 1 if x < 0 else -1)
        
        for ret in returns_list:
            df_temp = df[['signal_dto', ret]].dropna(how='any')
            ic_ratio = np.corrcoef(df_temp['signal_dto'], df_temp[ret])[0, 1]
            ic_ratio_list[f"{factor} for {ret}"] = ic_ratio
    ic_ratio_list = dict(sorted(ic_ratio_list.items(), key=lambda item: item[1], reverse=True))
    if plots:
        pass;
    return ic_ratio_list, df

def get_factor_returns(grouped_df, df) -> pd.DataFrame:
    # simulated returns
    grouped_df['回報_開盤15分鐘後5分鐘'] = get_factor(df, "10:15:00", "10:20:00", "1")
    grouped_df['回報_開盤15分鐘15分鐘'] = get_factor(df, "10:15:00", "10:25:00", "2")
    # factors
    grouped_df['因子_開盤後15分鐘'] = get_factor(df, "10:00:00", "10:15:00", "1")
    return grouped_df

def calc_plot(df):
    rolling = 20
    list_trading_date = sorted(list(set(df.da)))[rolling:]
    all_ic_df = []  # 用來存每天的 DataFrame
    
    for today in tqdm(list_trading_date):
        df_temp = df[df['da'] <= today]
        df_for_profit = df_temp[df_temp['time'] == "10:00:00"].set_index('da').iloc[-rolling:, :]
        df_for_profit = get_factor_returns(grouped_df=df_for_profit, df=df_temp)
        
        ic_ratio_list, df_for_profit = df_factor_profit_bt(
            df=df_for_profit,
            factors_list=factors_list,
            returns_list=returns_list
        )
        
        ic_df_today = pd.DataFrame.from_dict(ic_ratio_list, orient="index", columns=["IC"])
        ic_df_today["date"] = today
        all_ic_df.append(ic_df_today)
    
    ic_df = pd.concat(all_ic_df)
    
    ic_df = ic_df.reset_index().pivot(index="date", columns="index", values="IC")
    
    fig = go.Figure()
    for col in ic_df.columns:
        fig.add_trace(go.Scatter(
            x=ic_df.index,
            y=ic_df[col],
            mode="lines",
            name=col,
            visible='legendonly'
        ))
    fig.show()

if __name__ == "__main__":
    if not mt5.initialize():
        print("MT5 init failed", mt5.last_error())
    else:
        print("MetaTrader5 OK!", mt5.version())
        eurcad_rates = mt5.copy_rates_range(
            "GER40",
            mt5.TIMEFRAME_M5, 
            datetime.now() - timedelta(300), 
            datetime.now())
        df = pd.DataFrame(eurcad_rates)
        print(df)
        df = df.iloc[:, :5]
        df['time'] = pd.to_datetime(df['time'] * 1000000000)
        df['da'] = df['time'].apply(lambda x: x.strftime("%Y-%m-%d"))
        df['hms'] = df['time'].apply(lambda x: x.strftime("%H:%M:%S"))
        df.sort_values(by=['da', 'hms'], inplace=True)
        df = df.drop_duplicates(subset=['da', 'hms'])
        df = df.iloc[:, 1:]
        df.columns = ["op", "hi", "lo", "cl", "da", "time"]
        print(df.da)
        calc_plot(df)
        mt5.shutdown()
