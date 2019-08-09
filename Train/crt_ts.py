import proc_temperature as pt
import pandas as pd

def calc_ts_feat(df_dto):
    df_ts = df_dto.groupby(['DeliveryCustomerAccountKey']).apply(lambda x: create_time_series(x))
    df_ts = df_ts.reset_index().drop(['level_1'], axis=1)
    df_ts = pt.cum_hd_cd(df_ts)
    return df_ts

def create_time_series(df):#, df_hdd):
    '''
    Converts the original order data for each customer into a time series for each customer.
    The time series also includes heating degree days for the time period
    :param df: Data frame containing order history for each customer
    :param df_hdd: Data frame containing heating degree days
    :return: Time series cotaining features for every customer
    '''
    st_date = df['MovementDateKey'].min()
    en_date = df['MovementDateKey'].max()
    ord_date = df['MovementDateKey'].tolist()
    time_inx = pd.date_range(start=st_date, end=en_date, freq='D')
    df1 = pd.DataFrame(None,index=time_inx)
    df_ord = df.set_index('MovementDateKey')
    df1 = df1.join(df_ord.loc[:,['DispensedWeight', 'DispensedWeight1']])
    df1 = df1.reset_index()
    df1['DateKey'] = df1['index']
    del df1['index']
    df1['Order'] = 0
    df1.loc[df1['DateKey'].isin(ord_date), 'Order'] = 1
    return df1