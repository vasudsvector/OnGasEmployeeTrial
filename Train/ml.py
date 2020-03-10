import pandas as pd
from itertools import combinations


from sklearn.linear_model import Lasso



def ml(feat):
    '''Training a model happens here and the coefficients are trained here for each customer
    :param:feat - Machine learning Feature Matrix for each order
    :return df_coef - Linear regression coefficient for each customer
    '''
    ordercnt = feat.groupby('Customer')['OrderNum'].max()
    cols1 = feat.columns
    cols = cols1.drop(['Weight','OrderNum'])
    coef_cols = cols.drop(['Weight1','Customer'])

    df_coef = pd.DataFrame(None,columns=coef_cols)
    df_feat = pd.DataFrame(None,columns=cols)

    for cust in ordercnt.index:
        ls = range(1,ordercnt.loc[cust]+1)
        combin = combinations(ls, 2) # Generates combination between multiple orders irrespective of if they are in sequence or not and calculates total days between them

        for comb in combin: # Generates features for each combination
            feat1 = feat.loc[feat['Customer'] == cust,cols]
            feat2 = feat1.loc[feat['OrderNum'].between(comb[0]+1, comb[1]),:]
            feat3 = pd.DataFrame(feat2.sum(),index=cols).T
            feat3['Customer'] = cust
            df_feat = df_feat.append(feat3)

    customers = set(df_feat['Customer'])

    for cust in customers: # For each customer train a model and append it to coefficients dataframe
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