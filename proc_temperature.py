import pandas as pd

def proc_temperature(df_temp):
    '''
    Processes temperature data from datawarehouse to suit the algorithm needs
    :param df_temp: Temperature data downloaded from datawarehouse
    :return: Processed temperature data
    '''
    df_temp['At'] = df_temp['DateKey'].astype(str) + df_temp['TimeKey'] # Combine date and time columns as string
    df_temp['DateTime'] = pd.to_datetime(df_temp['At'], format='%Y%m%d%I:%M:%S %p') # Convert the combined datetime column into python DATETIME object
    df_temp = df_temp.drop(['DateKey', 'Year', 'Month', 'DayofMonth', 'TimeKey', 'At'], axis=1)
    df_temp['Date'] = [dat.date() for dat in df_temp['DateTime']] # Extract the date from DateTime Object
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
    '''
    :param df_rel1: Dataframe containing average temperature by day
    :param df_ts: Dataframe with timeseries index
    :return:
    '''
    df2 = df_ts.set_index('DateKey').join(df_rel1)
    df2['DispensedEnergy'] = df2['DispensedWeight'] # Energy is proxied by dispensed gas weight
    return df2


def calc_mean_temp(df_temp, df_ts, morn_peak=range(6,10), eve_peak = range(17,23)):
    '''
    Calculate the average temperature for each day
    :param df_temp: Dataframe containing temperature data
    :param df_ts: Dataframe with timeseries index for fn.rundates (Run dates of algorithm)
    :param morn_peak: Hours of morning peak
    :param eve_peak: Hours of evening peak
    :return: Average temperature for a given day is calculated by averaging the temperature at the morning and evening peaks
    '''
    df_temp['Hotd'] = [tim.time().hour for tim in df_temp['DateTime']] # hour of the day

    # Combine the morning and evening peak hours
    mp = list(morn_peak)
    ep = list(eve_peak)
    mp.extend(ep)

    # Retain temperature data corresponding to the time if it belongs to hour of interest (peak hours)
    df_rel = df_temp.loc[df_temp['Hotd'].isin(mp),:]

    df_rel1 = pd.DataFrame(None, index=pd.unique(df_rel['Date'])) # Init Dataframe

    df_rel1 = df_rel1.join(df_rel.groupby('Date').mean()['Tavg']) # Average the temperature by Date

    df_rel1.rename(columns={'Tavg':'Mean_Temp'}, inplace=True)

    if df_ts.empty:
        df_train = None
    else:
        df_train = _calc_auc(df_rel1, df_ts)
    return df_train, df_rel1