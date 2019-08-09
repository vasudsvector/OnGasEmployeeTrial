import pandas as pd
from sklearn.ensemble import RandomForestRegressor


import numpy as np
ls1 = np.histogram(df_rel1['Mean_Temp'].dropna(),bins=range(2,25,1))[0].tolist()
ls2 = [i/sum(ls1) for i in ls1]



df = pd.read_clipboard()
df1 = df.loc[2:,:]
tar = df1['QuantitativeEstimate']
df2 = df1.drop(['AccountNumber', 'QuantitativeEstimate'], 1)

df3 = pd.get_dummies(df2,columns=df2.columns[2:15])

rr = RandomForestRegressor()
rr.fit(df3,tar)

df_imp_rr = pd.DataFrame(rr.feature_importances_, index=df3.columns)/sum(rr.feature_importances_)
df_imp_rr['col'] = [ind.split('_')[0] for ind in df_imp_rr.index]
df_imp_rr.groupby('col').sum().sort_values(0)

from sklearn.linear_model import Lasso
ls = Lasso(positive=True, normalize=True, alpha=0.00001)
ls.fit(df3, tar)
df_imp = pd.DataFrame(ls.coef_, index=df3.columns)
df_imp['col'] = [ind.split('_')[0] for ind in df_imp.index]
df_imp.groupby('col').sum().sort_values(by=0)