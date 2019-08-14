import pandas as pd


def _fill_null_mean(dfwithnull):
    dfwithnull['DispensedWeight'].fillna(value=dfwithnull.loc[dfwithnull['MovementTypeKey'] == 'Fill','DispensedWeight'].mean(), inplace=True)
    return dfwithnull

def read_csv(file):
    '''
    Reads the data files
    :param file: filename along with location
    :return: Order data filtered to have orders post 30-Jun-2015 and 45 kg cylinders
    '''
    df = pd.read_csv(file)
    df = df.loc[df.OrderDateKey > 20100630, :]
    df_sub = df.loc[(df.OnGasProductKey == 2) | (df.OnGasProductKey == 69), :]
    return df_sub


def filter_data(df):
    '''
    Filters the data to have specific depot, order types, and customers having minimum 6 orders to 108 orders maximum
    :param df: Order Data
    :return: Subsetted Dataframe
    '''
    df_auck = df.loc[df['PriorityStatus'].isin(['Normal', 'Promised Date', 'Urgent', 'Unknown']), :]
    df_grp_cust1 = df_auck.groupby('DeliveryCustomerAccountKey').apply(lambda x: _fill_null_mean(x))
    df_grp_cust = df_grp_cust1.groupby('DeliveryCustomerAccountKey')
    cust_sub = df_grp_cust.apply(lambda x: True if (x.shape[0] >= 6 and x.shape[0] <= 108) else False)
    df_auck_sub = df_grp_cust1.loc[df_auck['DeliveryCustomerAccountKey'].isin(cust_sub.index[cust_sub]), :]
    df_auck_sub['OrderDateKey'] = pd.to_datetime(df_auck_sub['OrderDateKey'], format='%Y%m%d')
    df_auck_sub['MovementDateKey'] = pd.to_datetime(df_auck_sub['MovementDateKey'], format='%Y%m%d')
    df_auck_sub['DeliveryCustomerAccountKey'] = df_auck_sub['AccountNumber']
    return df_auck_sub