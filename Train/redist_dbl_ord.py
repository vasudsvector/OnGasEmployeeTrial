import pandas as pd


class RedistDouble:
    def __init__(self, doublesize):
        self.doublesize = doublesize

    def _helper_redist(self, x):
        wt = x['DispensedWeight']
        dbl_wt = wt.iloc[-1]
        if dbl_wt > self.doublesize:
            wt1 = wt + (dbl_wt - self.doublesize)/x.shape[0]
            wt1.iloc[-1] = self.doublesize+(dbl_wt - self.doublesize)/x.shape[0]
            x['DispensedWeight1'] = wt1
        else:
            x['DispensedWeight1'] = wt
        x.dropna(how='all',inplace=True)
        return x

    def redist_dbl_order(self, df_dto):
        '''
        The function redistributes the double order between last double order to all the single order uniformly.
        This is necessary as the consumption from the second cylinder is gradual and happens over some orders.
        It is not linearly distributed across all orders but thats the best known way to do it for training the model.
        :param df_dto: Dataframe containing Features of the order info
        :return: list containing dataframes with redistributed double orders
        '''
        df_cust_ts = df_dto.groupby('DeliveryCustomerAccountKey')
        ls_1 = []
        i=0
        for grp in df_cust_ts.groups:
            cust_ts = df_cust_ts.get_group(grp)
            cust_ts['DeliveryCustomerAccountKey'] = grp
            if cust_ts['DispensedWeight'].max() > self.doublesize:
                cust_ts.loc[:, 'doubleorder'] = 0
                cust_ts.loc[cust_ts['DispensedWeight'] > self.doublesize, 'doubleorder'] = 1
                cust_ts['doubleorder'] = cust_ts['doubleorder'].shift(1)
                cust_ts.loc[:, 'doubleorder'] = cust_ts['doubleorder'].cumsum()
                cust_ts = cust_ts.groupby(['doubleorder']).apply(lambda x: self._helper_redist(x))
            ls_1.extend([cust_ts])
        return ls_1