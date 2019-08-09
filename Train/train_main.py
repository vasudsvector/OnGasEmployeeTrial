import os
import read_filter as rf
import Train.feature as f
import Train.redist_dbl_ord as rd
import pandas as pd
import proc_temperature as pt
import Train.crt_ts as ct
import Train.MLE as mle
import ml
import sys
import dataproc as dp

folder = r'C:\Users\SurendranV\Projects\OnGas\Employeetrial\Data\\'
filename = r'OrderData1.csv'

morn_peak = (sys.argv[1]) if len(sys.argv) == 1 else range(6, 10)
eve_peak = (sys.argv[2]) if len(sys.argv) == 1 else range(17, 23)

bins = (sys.argv[3]) if len(sys.argv) == 1 else range(2,25,1)
doublesize = 45

cols = ['Customer', 'OrderNum'] + [i for i in bins][1:] + ['Weight', 'Weight1', 'days_to_order']

file = os.path.join(folder, filename)
df = rf.read_csv(file)
df = dp.handle_residual(df)
df = dp.assign_pickup_dates(df)
df_auck_sub = rf.filter_data(df)

# TODO Remove following two rows
rand_50_cus = ['992037096',
'992760513',
'992761565',
'992764766',
'992729448',
'992788996',
'992827378',
'992524526',
'992721238',
'990991692',
'991129164',
'991298890',
'992626386',
'992690960',
'992283717',
'992638039',
'991068621',
'992151505',
'992761916',
'992889140']
df_auck_sub = df_auck_sub.loc[df_auck_sub['DeliveryCustomerAccountKey'].isin(rand_50_cus), :]

df_dto = f.create_feat(df_auck_sub)
ro = rd.RedistDouble(doublesize=doublesize)
ls_dbl_redist = ro.redist_dbl_order(df_dto.drop('DeliveryCustomerAccountKey', 1).reset_index())
df_dbl_redist = pd.concat(ls_dbl_redist)
df_dbl_redist.dropna(how='all', inplace=True)
temp_file = r'./Data/temperature1.csv'
df_temp = pd.read_csv(temp_file)
df_temp = pt.proc_temperature(df_temp)
df_ts = ct.calc_ts_feat(df_dbl_redist)

df2, df_rel1 = pt.calc_mean_temp(df_temp, df_ts, morn_peak=morn_peak, eve_peak=eve_peak)

mle = mle.MLE(binsize = bins, cols = cols)
feat = mle.calc_ml_feat(df2)

# cust_coeff = feat.groupby('Customer').apply(lambda x: mle.linreg(x, tar_column='Weight1'))
# cust_coeff = pd.DataFrame(cust_coeff.values.tolist(), index=cust_coeff.index)
cust_coeff = ml.ml(feat)
cust_coeff.set_index('Customers', inplace=True)
cust_coeff.to_csv(r"Cust_coeff2.csv")