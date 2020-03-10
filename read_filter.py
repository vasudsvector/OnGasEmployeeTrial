import pandas as pd


def _fill_null_mean(dfwithnull):
    '''
    The name of the method is descriptive. It fills all the nulls with mean dispensed weight
    :param dfwithnull:
    :return: Filled dataframe
    '''
    dfwithnull['DispensedWeight'].fillna(value=dfwithnull.loc[dfwithnull['MovementTypeKey'] == 'Fill','DispensedWeight'].mean(), inplace=True)
    return dfwithnull

def read_csv(file):
    '''
    Reads the data files
    :param file: filename along with location
    :return: Order data filtered to have orders post 30-Jun-2010 and 45 kg cylinders
    '''
    df = pd.read_csv(file)
    df = df.loc[df.OrderDateKey > 20100630, :]
    df_sub = df.loc[(df.OnGasProductKey == 2) | (df.OnGasProductKey == 69), :]
    return df_sub


def filter_data(df):
    '''
    Filters the data to have specific depot, order types, and customers having minimum 2 orders to 108 orders maximum
    :param df: Order Data
    :return: Subsetted Dataframe
    '''
    df_auck = df.loc[df['PriorityStatus'].isin(['Normal', 'Promised Date', 'Urgent', 'Unknown']), :] # Fulfilment type
    df_grp_cust1 = df_auck.groupby('DeliveryCustomerAccountKey').apply(lambda x: _fill_null_mean(x)) # Fill null values by customer average
    df_grp_cust = df_grp_cust1.groupby('DeliveryCustomerAccountKey')
    cust_sub = df_grp_cust.apply(lambda x: True if ((x['DispensedWeight'].count()) >= 2 and (x['DispensedWeight'].count()) <= 108) else False) # At least 2 orders and max of 108 orders
    df_auck_sub = df_grp_cust1.loc[df_auck['DeliveryCustomerAccountKey'].isin(cust_sub.index[cust_sub]), :]
    # Convert Date String to DateTime object
    df_auck_sub['OrderDateKey'] = pd.to_datetime(df_auck_sub['OrderDateKey'], format='%Y%m%d')
    df_auck_sub['MovementDateKey'] = pd.to_datetime(df_auck_sub['MovementDateKey'], format='%Y%m%d')
    df_auck_sub['DeliveryCustomerAccountKey'] = df_auck_sub['AccountNumber']
    return df_auck_sub