import pandas as pd

def proc_temperature(df_temp):
    '''
    :param df_temp: Temperature data downloaded from datawarehouse
    :return: Processed temperature data
    '''
    df_temp['At'] = df_temp['DateKey'].astype(str) + df_temp['TimeKey']
    df_temp['DateTime'] = pd.to_datetime(df_temp['At'], format='%Y%m%d%I:%M:%S %p')
    df_temp = df_temp.drop(['DateKey', 'Year', 'Month', 'DayofMonth', 'TimeKey', 'At'], axis=1)
    df_temp['Date'] = [dat.date() for dat in df_temp['DateTime']]
    return df_temp

def _calc_dd(df, cold_thresh, hot_thresh):
    df['hdd'] = df.loc[df['Text'] <= cold_thresh, 'Text'].sum()
    df['cdd'] = df.loc[df['Text'] >= hot_thresh, 'Text'].sum()
    return df

def cum_hd_cd(df_ts):
    '''
    Calculate time series temperature features for each customer
    :param df_ts: Time series of order date for each customer
    :return: Time series with cumulated degree days for each customer
    '''
    cust = df_ts['DeliveryCustomerAccountKey']
    df_ts['order_num'] = df_ts.groupby('DeliveryCustomerAccountKey')['Order'].cumsum()
    df_ts['DeliveryCustomerAccountKey'] = cust
    return df_ts

def _calc_auc(df_rel1, df_ts):
    df2 = df_ts.set_index('DateKey').join(df_rel1)
    df2['DispensedEnergy'] = df2['DispensedWeight']
    return df2


def calc_mean_temp(df_temp, df_ts, morn_peak=range(6,10), eve_peak = range(17,23)):
    df_temp['Hotd'] = [tim.time().hour for tim in df_temp['DateTime']] # hour of the day
    mp = list(morn_peak)
    ep = list(eve_peak)
    mp.extend(ep)
    df_rel = df_temp.loc[df_temp['Hotd'].isin(mp),:]
    df_rel1 = pd.DataFrame(None, index=pd.unique(df_rel['Date']))
    df_rel1 = df_rel1.join(df_rel.groupby('Date').mean()['Tavg'])
    df_rel1.rename(columns={'Tavg':'Mean_Temp'}, inplace=True)
    if df_ts.empty:
        df2 = None
    else:
        df2 = _calc_auc(df_rel1, df_ts)
    return df2, df_rel1