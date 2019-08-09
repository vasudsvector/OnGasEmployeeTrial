import pandas as pd
from itertools import combinations


from sklearn.linear_model import Lasso



def ml(feat):
    ordercnt = feat.groupby('Customer')['OrderNum'].max()
    cols1 = feat.columns
    cols = cols1.drop(['Weight','OrderNum'])
    coef_cols = cols.drop(['Weight1','Customer'])

    df_coef = pd.DataFrame(None,columns=coef_cols)
    df_feat = pd.DataFrame(None,columns=cols)

    for cust in ordercnt.index:
        ls = range(1,ordercnt.loc[cust]+1)
        combin = combinations(ls, 2)

        for comb in combin:
            feat1 = feat.loc[feat['Customer'] == cust,cols]
            feat2 = feat1.loc[feat['OrderNum'].between(comb[0]+1, comb[1]),:]
            feat3 = pd.DataFrame(feat2.sum(),index=cols).T
            feat3['Customer'] = cust
            df_feat = df_feat.append(feat3)

    customers = set(df_feat['Customer'])

    for cust in customers:
        df_cust = df_feat.loc[df_feat['Customer']==cust,:]
        lin = Lasso(alpha=0.0001, precompute=True, max_iter=1000,
                    positive=True, random_state=9999, selection='random', fit_intercept=False)
        lin.fit(df_cust.drop(['Weight1', 'Customer'], 1), df_cust['Weight1'])
        df_coef1 = pd.DataFrame(lin.coef_, index=coef_cols).T
        df_coef1['Customers'] = cust
        df_coef = df_coef.append(df_coef1)

    return df_coef

if __name__ == '__main__':
    feat = pd.read_csv(r'Features.csv')
    df_coef = ml(feat)