import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import LinearSVR
import proc_temperature as pt

lr = LinearRegression(fit_intercept=False)
rf = RandomForestRegressor()
svr = LinearSVR()
import Train.split_by_season as ss

# Energy in bottle
tot_en = 2212  # in MJ, 45kg*1.856*26
base_cons = 4.5  # 9MJ burner burning for half an hour everyday
heater_max_config = 15  # MJ, can be 25 as well
heater_duration = 4  # hours
heater_energy_cons = heater_max_config * heater_duration
hot_water_max_config = 125  # MJ/hour for 16 L/Min hot water and 199 MJ/hour for 26L/Min hotwater
hot_water_duration = 5  # Minutes
hw_energy_cons = hot_water_max_config * hot_water_duration / 60
max_cons = hw_energy_cons + heater_energy_cons + base_cons

alpha = 12  # Temperature below which heater and hot water will start pushing the consumption up the slope
beta = 6  # Temperature at which max_cons is being consumed at the house
gamma = (max_cons - base_cons) / (alpha - beta)


class MLE:
    def __init__(self, binsize, cols):
        self.binsize = binsize
        self.cols = cols

    def hist_grp(self, x):
        y = x['Mean_Temp']
        hist1 = np.histogram(y[np.isfinite(y)], bins=self.binsize)[0]  # , density=True
        hist1 = hist1  # /days
        # print(hist1)
        return hist1

    def crt_tar(self, x):
        hist1 = self.hist_grp(x)
        days = x['Mean_Temp'].count()
        df = pd.DataFrame(hist1).T
        df['tar'] = x['DispensedWeight'].sum()
        df['tar1'] = x['DispensedWeight1'].sum()
        df['days'] = days
        return df

    def shift_grp(self, df_cust):
        df_cust['Weight'] = df_cust['Weight'].shift(-1)
        df_cust['Weight1'] = df_cust['Weight1'].shift(-1)
        return df_cust

    def calc_ml_feat(self, df_train):
        '''
        Create all the Machine Learning Features necessary for training a linear regressor
        :param df_train: Order information corrected for double orders
        :return: Machine learning feature for each order
        '''
        df_train.loc[df_train['DispensedWeight1'].isnull(), 'DispensedWeight1'] = df_train.loc[
            df_train['DispensedWeight1'].isnull(), 'DispensedWeight']
        feat = df_train.groupby(['DeliveryCustomerAccountKey', 'order_num'])[
            'DispensedWeight1', 'DispensedWeight', 'Mean_Temp'].apply(lambda x: self.crt_tar(x))
        feat = feat.droplevel(level=2, axis=0)
        feat = feat.drop_duplicates().reset_index()
        feat.columns = self.cols
        feat = feat.groupby(['Customer']).apply(lambda x: self.shift_grp(x))
        feat = feat.dropna(subset=['Weight'])
        return feat



if __name__ == '__main__':
    morn_peak = range(6, 10)
    eve_peak = range(17, 23)
    temp_file = r'./Data/temperature.csv'
    df_temp = pd.read_csv(temp_file)
    df_temp = pt.proc_temperature(df_temp)
    temp_cust = ss.seas_split(df_dto)
    df2, df_rel1 = pt.calc_mean_temp(df_temp, df_ts, alpha, morn_peak=morn_peak, eve_peak=eve_peak)

    feat, feat2, tar = calc_ml_feat(df2, temp_cust, spc_heat=True)
    cust_coeff = feat.groupby('Customer').apply(lambda x: linreg(x, tar_column='Weight1'))
    cust_coeff = pd.DataFrame(cust_coeff.values.tolist(), index=cust_coeff.index)
