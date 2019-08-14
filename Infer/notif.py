import os
import pandas as pd
import read_filter as rf
from datetime import date, timedelta, datetime
import json

from Infer.pred_consumption import Customers
import proc_temperature as pt
from Infer.handle_notifications import Order_Msg

## Read files and define variable values ##
df_ts = pd.DataFrame()
morn_peak = range(7, 10)
eve_peak = range(19, 23)
dbl_ord_qty = 90


class FuncNotif():
    def __init__(self, custids, last_run_date, run_date, coeff=None, dct_state=None, employeetrial=True,
                 bins=range(5, 25, 3)):
        self.run_date = pd.to_datetime(run_date, format='%d/%m/%Y')
        try:
            self.last_run_date = str(pd.to_datetime(last_run_date, format='%d/%m/%Y').date())
        except:
            self.last_run_date = str(pd.to_datetime(last_run_date, format='%Y-%m-%d').date())

        self.rundates = pd.date_range(pd.to_datetime(self.last_run_date),
                                      (self.run_date - timedelta(1)))  # (last_run_date, date.today())
        self.df_1 = pd.DataFrame(None, columns=['disp_wt', 'cons', 'Date'])
        self.custids = custids
        self.trial = employeetrial
        self.bins = bins

        if not (coeff.empty):
            self.df_coeffs = coeff
        else:
            # TODO Train new coefficients from order data
            pass
        if 'cust_notified_already' in dct_state:
            self.dct_state = dct_state
        else:
            self.dct_state = {}
            self.dct_state['cust_notified_already'] = set()
            self.dct_state['last_run_date'] = self.last_run_date

    def read_state(self, state_loc):
        df_cumulative_consumption = pd.read_csv(state_loc)
        df_cumulative_consumption = df_cumulative_consumption.rename({'Unnamed: 0': 'Customer'}, axis=1)
        df_cumulative_consumption.set_index('Customer', inplace=True)
        return df_cumulative_consumption

    def read_last_run(self, dct_state):
        last_run_date = dct_state['last_run_date']
        cust_notified_already = dct_state['cust_notified_already']
        return last_run_date, cust_notified_already

    def fetch_temp(self, df_temp):
        df_temp = pt.proc_temperature(df_temp)
        df2, df_rel1 = pt.calc_mean_temp(df_temp, df_ts, morn_peak=morn_peak, eve_peak=eve_peak)
        return df_rel1

    def fetch_empty_cust(self, msg, df_cumulative_consumption, daily_cons):
        df_cumulative_consumption.fillna(0, inplace=True)
        df_cumulative_consumption['Cumulative_Consumption'] = df_cumulative_consumption['Cumulative_Consumption'] + \
                                                              daily_cons
        cust_ready_to_order, cust_emptied_both = msg.categorise_customers(
            df_cumulative_consumption)  # Find customers who emptied one and both bottles on this day
        return cust_ready_to_order, cust_emptied_both, df_cumulative_consumption

    def _write_state_out(self, df_cumulative_consumption, rundate, cust_notify_tdy, cust_notified_already):
        self.dct_state['Cumulative_Consumption'] = df_cumulative_consumption.to_json()
        self.dct_state[str(rundate)] = list(cust_notify_tdy)
        self.dct_state['cust_notified_already'] = list(cust_notified_already)
        self.dct_state['cust_notify_today'] = list(cust_notify_tdy)

    def error_consum(self, disp_weight_rel, df_cumulative_consumption, date1):
        disp_weight_rel = disp_weight_rel.rename({'DispensedWeight': 'Cumulative_Consumption'}, axis=1)
        df_3 = pd.DataFrame()
        df_3['pred_cons'] = (df_cumulative_consumption.loc[disp_weight_rel.index, 'Cumulative_Consumption'])
        df_3['disp_wt'] = (disp_weight_rel['Cumulative_Consumption'])
        df_3['Date'] = date1
        df_3['diff'] = (df_3['pred_cons'] - df_3['disp_wt']) * 100 / df_3['disp_wt']
        self.df_1 = self.df_1.append(df_3)

    def notif(self, date1, cust_ent_date, custids, mean_temp, df_order, dct_state, cons):
        rundate = date1.date()  # (date.today()-timedelta(1))
        last_run_date, cust_notified_already = self.read_last_run(dct_state)  # to be checked

        # Estimate the daily consumption for date1 for all relevant customers
        pres_cons = cust_ent_date.loc[cust_ent_date['Join_Date'] <= pd.Timestamp(rundate)].index.tolist()
        rel_cust_run = [cust for cust in pres_cons if cust in custids]

        customer = Customers(rel_cust_run, self.df_coeffs, self.bins)

        runday_cons = customer.daily_cons(mean_temp)

        cons.loc[runday_cons.index, rundate] = runday_cons

        dct_cumulative_consumption = json.loads(dct_state['Cumulative_Consumption'])
        df_cumulative_consumption = pd.DataFrame(dct_cumulative_consumption)
        df_cumulative_consumption.index = df_cumulative_consumption.index.astype(float)  # JSON DUMPED to str
        msg = Order_Msg(run_date=rundate, last_run_date=last_run_date, ord_qty=None, dbl_ord_qty=None)
        cust_ready_to_order, cust_emptied_both, df_cumulative_consumption = self.fetch_empty_cust(msg,
                                                                                                  df_cumulative_consumption,
                                                                                                  cons[rundate])

        # cust_ready_to_order - Customers whose cumulative consumption is between 45kg and 90kg
        # cust_emptied_both - Customers whose cumulative consumption exceeded 90kg
        #

        # People who have already ordered need not be notified instead their predicted consumption should be corrected, find them
        cust_due_notif, cust_ordered_last_day, disp_weight = msg.remove_already_ordered(df_order,
                                                                                        cust_ready_to_order,
                                                                                        cust_emptied_both)
        # cust_due_notif -

        # Who should be notified today?
        cust_notify_tdy = cust_due_notif - set(cust_notified_already)  # Exclude already notified customers
        cust_notified_already = set(cust_notified_already)
        cust_notified_already.update(cust_ready_to_order)

        check = any(item in custids for item in disp_weight.index)
        if check:
            disp_weight_rel = pd.DataFrame(disp_weight.loc[disp_weight.index.isin(custids), 'DispensedWeight'])
            self.error_consum(disp_weight_rel, df_cumulative_consumption, rundate)

        cust_deliv_last_day = set(disp_weight.index)
        cust_notified_already = (cust_notified_already - cust_deliv_last_day)

        if not (disp_weight.empty) and any(item in custids for item in cust_deliv_last_day):
            disp_weight['Cumulative_Consumption'] = disp_weight.loc[disp_weight.index.isin(custids), :]
            df_cumulative_consumption.fillna(0, inplace=True)
            if not self.trial:
                df_cumulative_consumption.loc[
                    df_cumulative_consumption.index.isin(cust_deliv_last_day), 'Cumulative_Consumption'] = \
                    df_cumulative_consumption.loc[
                        df_cumulative_consumption.index.isin(cust_deliv_last_day), 'Cumulative_Consumption'] - \
                    disp_weight.loc[disp_weight.index.isin(cust_deliv_last_day), 'Cumulative_Consumption']
            elif self.trial:
                df_cumulative_consumption.loc[
                    df_cumulative_consumption.index.isin(cust_deliv_last_day), 'Cumulative_Consumption'] = 0

        # TODO New module or object needs to be created after cumulative consumption calculation
        df_cumulative_consumption['Cumulative_Consumption'] = df_cumulative_consumption[
            'Cumulative_Consumption'].clip(lower=0, upper=dbl_ord_qty)

        self._write_state_out(df_cumulative_consumption, rundate, cust_notify_tdy, cust_notified_already)
        return cons, self.dct_state


if __name__ == '__main__':
    ### Define Variables ###
    ## File Locations ##

    # Inputs
    coeff_loc = r'./Data/Customer_Coefficients.csv'
    folder = r'./Data/'
    filename = r'CustomerOrders-activecustomers-normalorders-04Apr2019.csv'
    custentdatefile = r'./Data/Customer_First_Entry_Date.csv'
    temp_file = r'./Data/temperature.csv'

    # Input and Output
    state_loc = r'state_file.csv'
    cons_file = r'./Data/calc_consumption.csv'
    notify_file = r'./Data/notify.json'

    order_loc = os.path.join(folder, filename)

    df_order = rf.read_csv(order_loc)
    df_temp = pd.read_csv(temp_file)

    custids = [184402.0, 303224.0, 315103.0, 372171.0, 373548.0, 504768.0,
               1048392.0, 1240691.0, 1592741.0, 1681144.0, 1760507.0, 1798780.0,
               1836847.0, 2430166.0, 2848345.0, 2881646.0, 2896783.0, 2928320.0,
               3027296.0, 3123743.0, 3148573.0]
    last_run_date = '2010/06/30'
    run_date = '2016/07/31'

    df_state = pd.DataFrame(0, index=custids, columns=['Cumulative_Consumption'])
    df_state.to_csv(state_loc)
    dct_state = {}
    dct_state['last_run_date'] = pd.to_datetime(last_run_date)
    dct_state['cust_notified_already'] = set()
    df_coeffs = pd.read_csv(coeff_loc)

    fn = FuncNotif(custids, last_run_date, run_date, coeff=df_coeffs, dct_state=dct_state)

    df_auck_sub = rf.filter_data(df_order)

    cust_ent_date = pd.read_csv(custentdatefile, header=None, names=['Customer', 'Join_Date'])
    cust_ent_date.set_index('Customer', inplace=True)
    cust_ent_date['Join_Date'] = pd.to_datetime(cust_ent_date['Join_Date'])

    custlist1 = cust_ent_date.loc[cust_ent_date['Join_Date'] <= fn.rundates[-1]].index.tolist()
    custlist = [cust for cust in custlist1 if cust in custids]
    cons = pd.DataFrame(None, index=custlist,
                        columns=fn.rundates)  # Init df - df containing daily consumption for all dates in the current run
    lat_cons = pd.DataFrame()
    cust_notified_already = set()  # dct_state['cust_notified_already'] # Stored from previous run
    df_coeffs = pd.read_csv(coeff_loc)

    cust_ent_date = pd.read_csv(custentdatefile, header=None, names=['Customer', 'Join_Date'])
    cust_ent_date.set_index('Customer', inplace=True)
    cust_ent_date['Join_Date'] = pd.to_datetime(cust_ent_date['Join_Date'])

    df_temp_rel = fn.fetch_temp(df_temp)
    for date1 in fn.rundates:  # Run it for all days between previous run and current run
        mean_temp = df_temp_rel.loc[
            date1.date(), 'Mean_Temp']  # Mean temperature of the day, calculated during peak hours
        lat_cons = fn.notif(date1=date1, cust_ent_date=cust_ent_date, custids=custids, mean_temp=mean_temp,
                            df_order=df_auck_sub, dct_state=dct_state, cons=cons)

    fn.dct_state['last_run_date'] = str(date1.date())
    with open(notify_file, 'w') as fw:
        json.dump(fn.dct_state, fw)

    lat_cons.to_csv(cons_file)
    # df2 = fn.df_1
    # fn.df_1.to_csv('Error.csv')
    # dct_state = fn.read_state(state_loc)
