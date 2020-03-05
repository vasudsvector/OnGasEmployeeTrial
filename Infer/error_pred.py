import pandas as pd
import read_filter as rf
from datetime import timedelta

class error_pred():
    def __init__(self, df_order, cons, rundates):
        self.df_order = df_order
        self.cons = cons.T
        if self.cons.index.name != 'Date':
            self.cons.set_index('Date', inplace=True)
        self.cons.columns = [int(col) for col in self.cons.columns]
        self.cons.index = pd.to_datetime(self.cons.index)
        self.rundates = rundates

    def filter_orders_by_date(self):
        df_ord = self.df_order.loc[self.df_order['MovementDateKey'].between(self.rundates[0], self.rundates[-1],True),:]
        return df_ord

    def __grp_oper(self,grp):
        grp['MovementDateKey'] = grp['MovementDateKey'].min()
        return grp

    def calc_act_consump(self, df_ord):
        df_del_fill = df_ord.loc[df_ord['MovementTypeKey'].isin([10,16]),:]
        df_grp = df_del_fill.groupby(['DeliveryCustomerAccountKey', 'CylinderKey']).apply(lambda x: self.__grp_oper(x))
        df_act_consum = df_grp.groupby(['DeliveryCustomerAccountKey', 'MovementDateKey']).sum()['DispensedWeight'].reset_index()
        return df_act_consum

    def get_last_order_date(self, cust,date):
        df_del = self.df_order.loc[self.df_order['MovementTypeKey']==16, :]
        df_del  = df_del.loc[df_del['DeliveryCustomerAccountKey'].isin([cust]),:]
        ord_dates  = sorted(df_del.loc[df_del['MovementDateKey'] < date, 'MovementDateKey'])
        if ord_dates:
            last_ord_date = ord_dates[-1]
        else:
            last_ord_date = None
        return last_ord_date

    def __get_cust_pred_cons(self, x):
        cons = self.cons
        cust_cons = cons.loc[pd.date_range(x['last_order_date'],x['MovementDateKey']), x.name].sum()
        return cust_cons


    def get_pred_cons(self, df_consum):
        pred_cons = df_consum.apply(lambda x: self.__get_cust_pred_cons(x), axis=1)
        pred_cons = pd.DataFrame(pred_cons)
        pred_cons['MovementDateKey'] = df_consum['MovementDateKey']
        return pd.DataFrame(pred_cons)



if __name__ == '__main__':
    df_order = pd.read_csv(r'C:\Users\SurendranV\Projects\OnGas\Employeetrial_handover\Data\Input\OrderData1.csv')
    df_order = rf.filter_data(df_order)
    df_cons = pd.read_csv(r'C:\Users\SurendranV\Projects\OnGas\Employeetrial_handover\Data\Debug\1_simp_avg\cons.csv')
    rundates = pd.date_range(pd.to_datetime('07/10/2019'),
                                  (pd.to_datetime('23/08/2019') - timedelta(1)))
    ep = error_pred(df_order, df_cons, rundates)
    df_ord = ep.filter_orders_by_date()
    df_consum = ep.calc_act_consump(df_ord)
    cust = df_consum['DeliveryCustomerAccountKey']
    df_dates = ep.get_last_order_date(cust)
    df_consum.set_index('DeliveryCustomerAccountKey', inplace=True)
    df_consum = df_consum.join(df_dates)
    df_consum.rename({0:'last_order_date'},axis=1, inplace=True)
    df_consum = df_consum.loc[df_consum['MovementDateKey'] >= pd.to_datetime('08/01/2019'),:]
    pred_cons = ep.get_pred_cons(df_consum)
    df_error = pd.merge(df_consum.reset_index(), pred_cons.reset_index(), on=['DeliveryCustomerAccountKey','MovementDateKey'])
    df_error['Error'] = (df_error['DispensedWeight'] - df_error[0])*100/df_error['DispensedWeight']
