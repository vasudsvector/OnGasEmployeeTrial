import pandas as pd
import numpy as np
import os
import read_filter as rf


class Customers():
    def __init__(self, custids, df_coeffs, bins):
        self.custids = custids
        self.coeffs = self.fetch_coeff(df_coeffs, custids)
        self.bins = bins

    def fetch_coeff(self, df_coeffs, custids):
        if 'Customers' in df_coeffs.columns:
            df_coeffs.set_index('Customers', inplace=True)
            coeffs = df_coeffs.loc[custids, :]
        elif 'Customers' in df_coeffs.index.name:
            coeffs = df_coeffs.loc[custids, :]
        else:
            raise ValueError('Customer column missing in Coefficient File')
        return coeffs

    def daily_cons(self, temp_today):
        temp = np.histogram(temp_today, bins=self.bins)[0]
        temp = np.insert(temp, temp.shape[0], np.sum(temp))
        cons = self.coeffs.dot(temp)
        cons[cons < 0] = 0
        return cons


if __name__ == '__main__':
    coeff_loc = r'Customer_Coefficients.csv'
    state_loc = r'state_file.csv'
    folder = r'C:\Users\SurendranV\Projects\OnGas\Customer_Reorder_Prediction\Data\\'
    filename = r'CustomerOrders-activecustomers-normalorders-04Apr2019.csv'
    custids = [184402.0, 303224.0, 315103.0, 372171.0, 373548.0, 504768.0,
               1048392.0, 1240691.0, 1592741.0, 1681144.0, 1760507.0, 1798780.0,
               1836847.0, 2430166.0, 2848345.0, 2881646.0, 2896783.0, 2928320.0,
               3027296.0, 3123743.0, 3148573.0]
    temp_today = 18

    file = os.path.join(folder, filename)
    cold_start = False
    df_coeffs = pd.read_csv(coeff_loc)

    if cold_start:
        df = rf.read_csv(file)
        df_auck_sub = rf.filter_data(df)
        df_state = pd.DataFrame(0, index=custids, columns=['Cumulative_Consumption'])
    else:
        df_state = pd.read_csv(state_loc)
        df_state = df_state.rename_axis({'Unnamed: 0': 'Customer'}, axis=1)
        df_state.set_index('Customer', inplace=True)
        customer = Customers(custids, df_coeffs)
        df_state = customer.daily_cons(temp_today)
