import pandas as pd
from scipy.stats import mannwhitneyu

def seas_split(df_dto):
    w = [6, 7, 8]
    s = [12, 1, 2]
    df_dto['month'] = df_dto['OrderDateKey'].dt.month
    df_dto['Season'] = 'o'
    df_dto.loc[df_dto['month'].isin(s), 'Season'] = 's'
    df_dto.loc[df_dto['month'].isin(w), 'Season'] = 'w'

    dct = {}
    for cust in pd.unique(df_dto['DeliveryCustomerAccountKey']):
        df_cust = df_dto.loc[df_dto['DeliveryCustomerAccountKey'] == cust, :]
        df_cust_s = df_cust.loc[df_cust['Season'] == 's', 'days_to_order']
        df_cust_w = df_cust.loc[df_cust['Season'] == 'w', 'days_to_order']
        try:
            pval = (mannwhitneyu(df_cust_s, df_cust_w)[1])
        except ValueError as e:
            print(e, ' in cust')
            continue
        if pval < 0.05:
            dct[cust] = pval

    temp_cust = list(dct.keys())
    return temp_cust