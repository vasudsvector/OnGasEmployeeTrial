import pandas as pd
import datetime
import proc_temperature as pt
import numpy as np
from dateutil.relativedelta import relativedelta
from matplotlib import pyplot as plt

morn_peak = range(6, 10)
eve_peak = range(17, 23)

today = datetime.datetime.today().date()
six_months = today - relativedelta(months=6)

# df_ts = pd.DataFrame()
alpha = 18

temp_file = r'./Data/temperature.csv'
df_temp = pd.read_csv(temp_file)
df_temp = pt.proc_temperature(df_temp)


def hist1(x):
    return np.histogram(x, bins=range(5, 25, 3))[0]


def pred(x, coeff):
    day_cons = x.dot(coeff)
    return day_cons


def _mean_absolute_percentage_error(y_true, y_pred):
    ## Note: does not handle mix 1d representation
    # if _is_1d(y_true):
    #    y_true, y_pred = _check_1d_array(y_true, y_pred)

    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100


def calc_mape(df_cust):
    mape = _mean_absolute_percentage_error(
        df_cust.loc[~(df_cust['DispensedWeight'].isnull()), 'Cumulative_Dispensed_Weight'],
        df_cust.loc[~(df_cust['DispensedWeight'].isnull()), 'Cumulative_Consumption'])
    return mape


def plt_figs(df_cust, cat, cust):
    fig, ax = plt.subplots()
    ax1 = ax.twinx()
    df_cust.loc[:, ['Cumulative_Consumption', 'Cumulative_Dispensed_Weight']].plot(ax=ax, color=['g', 'c'])
    df_cust['resid'].plot(secondary_y=True, ax=ax1, c='b')
    mape = calc_mape(df_cust)
    ax.set_xlabel('Date')
    ax.set_ylabel('Gas Consumption in kg')
    ax1.set_ylabel('Residual')
    plt.savefig('.\Figs\\' + cat + '_' + str(cust) + '_' + str(mape) + '.png')
    return mape


def calc_cyl_cnt(df_cust):
    df_cust['cyl_count'] = np.floor(df_cust['Cumulative_Consumption'] / 43)
    df_cust.loc[df_cust['DispensedWeight'].isnull(), 'order'] = 0
    ser = df_cust.loc[~(df_cust['DispensedWeight'].isnull()), 'DispensedWeight']
    ls = [2 if qty > 45 else 1 for qty in ser]
    df_cust.loc[df_cust['order'].isnull(), 'order'] = ls
    df_cust['order_num'] = df_cust['order'].cumsum()
    df_cust['cyl_count'] = np.floor(df_cust['Cumulative_Consumption'] / 43)
    return df_cust

def meas_date_error(df_cust):
    final_diff = (df_cust['cyl_count'] - df_cust['order_num'])[-1]
    if abs(final_diff) <= 2:
        df_cyl_grp = df_cust.groupby('cyl_count')
        dct1 = {grp:df_cyl_grp.get_group(grp).index[0] for grp in df_cyl_grp.groups}
        df = pd.DataFrame(dct1.values(), index=dct1.keys(), columns=['Predicted_Order_Date'])
        df_cyl_grp = df_cust.groupby('order_num')
        dct1 = {grp: df_cyl_grp.get_group(grp).index[0] for grp in df_cyl_grp.groups}
        df1 = pd.DataFrame(dct1.values(), index=dct1.keys(), columns=['Actual_Order_Date'])
        df = df.join(df1)
        df['act_ord_date_s'] = df['Actual_Order_Date'].shift(1)
        df['act_ord_date_s'] = df['act_ord_date_s'].ffill()
        df['dto'] = df['Actual_Order_Date'] - df['act_ord_date_s']
        df['error'] = df.iloc[:, 0] - df.iloc[:, 1]
        return df
    else:
        return pd.DataFrame()



if __name__ == '__main__':
    dct = {}
    ls = []
    cust_ord_date = df_auck_sub.groupby('DeliveryCustomerAccountKey').apply(lambda x: x.iloc[0, :]['MovementDateKey'])
    df2, df_rel1 = pt.calc_mean_temp(df_temp, df_ts, alpha, morn_peak, eve_peak)
    df_bin_mt = df_rel1['Mean_Temp'].apply(lambda x: hist1(x))
    df_bin_mt = pd.DataFrame(df_bin_mt.values.tolist(), index=df_rel1.index)
    df_bin_mt[6] = df_bin_mt.sum(1)

    for cust in cust_ord_date.index:
        st_date = cust_ord_date[cust].date()
        df_cust = df_bin_mt.loc[st_date:, :]
        try:
            coeff = cust_coeff.loc[cust, :]
            cat = 'therm'
        except:
            coeff = cust_coeff1.loc[cust, :]
            cat = 'non_therm'
            pass
        df_cust['day_cons'] = df_cust.apply(lambda x: pred(x, coeff), axis=1)
        df_cust.loc[df_cust['day_cons'] < 0, 'day_cons'] = 0
        df_cust['Cumulative_Consumption'] = df_cust['day_cons'].cumsum()
        df_act_cust = df_dto.loc[df_dto['DeliveryCustomerAccountKey'] == cust, ['MovementDateKey', 'DispensedWeight']]
        df_act_cust.set_index('MovementDateKey', inplace=True)
        df_cust = df_cust.join(df_act_cust)
        df_cust['Cumulative_Dispensed_Weight'] = df_cust['DispensedWeight'].cumsum()
        df_cust['Cumulative_Dispensed_Weight'] = df_cust['Cumulative_Dispensed_Weight'].ffill()
        df_cust['Cumulative_Consumption'] = df_cust['Cumulative_Consumption'] + df_cust['DispensedWeight'][0]
        df_cust['resid'] = df_cust['Cumulative_Dispensed_Weight'] - df_cust['Cumulative_Consumption']
        df_cust = calc_cyl_cnt(df_cust)
        df_cust.loc[:, ['cyl_count', 'order_num']].plot()
        df_err = meas_date_error(df_cust)
        if df_err.empty:
            pass
        else:
            print('For customer {}, error in days is {}'.format(cust, np.abs(df_err['error']).mean() / df_err['dto'].mean()))
            df_err.to_csv(str(cust)+'.csv')

        mape = plt_figs(df_cust, cat, cust)
        ls.extend([mape])
        dct[cust] = df_cust
