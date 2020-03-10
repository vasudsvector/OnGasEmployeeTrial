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
    '''
    This class is the workhorse of the prediction app
    It does following things
    1) Reads data generated from last run if it exists
    2) Fetches temperature data and calculates average for a day at given timeperiods
    '''
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

    def read_last_run(self, dct_state):
        '''
        Read information from state file created during last day execution or from initiated state
        :param dct_state: dictionary containing state
        :return: last_run_date - Date of last execution
        cust_notified_already - list of customers who has been notified already
        '''
        last_run_date = dct_state['last_run_date']
        cust_notified_already = dct_state['cust_notified_already']
        return last_run_date, cust_notified_already

    def fetch_temp(self, df_temp):
        '''
        :param df_temp: Temperature data as downloaded from datawarehouse
        :return: Average temperature values for each day (averaged at peak hours)
        '''
        df_temp = pt.proc_temperature(df_temp)
        df2, df_rel1 = pt.calc_mean_temp(df_temp, df_ts, morn_peak=morn_peak, eve_peak=eve_peak)
        return df_rel1

    def fetch_empty_cust(self, msg, df_cumulative_consumption, daily_cons):
        '''
        :param msg: Instantiated Order_Msg class object
        :param df_cumulative_consumption: Cumulative consumption dataframe for each customer
        :param daily_cons: Consumption calculated for all customers for today
        :return:
        '''
        df_cumulative_consumption.fillna(0, inplace=True)
        df_cumulative_consumption['Cumulative_Consumption'] = df_cumulative_consumption['Cumulative_Consumption'] + \
                                                              daily_cons
        cust_ready_to_order, cust_emptied_both = msg.categorise_customers(df_cumulative_consumption)  # Find customers who emptied one and both bottles on this day
        return cust_ready_to_order, cust_emptied_both, df_cumulative_consumption

    def _write_state_out(self, df_cumulative_consumption, rundate, cust_notify_tdy, cust_notified_already):
        '''
        State file is formulated and formatted here.
        :param df_cumulative_consumption: Predicted Cumulative consumption for each customer
        :param rundate: Date of execution of code
        :param cust_notify_tdy: Who needs to be notified today
        :param cust_notified_already: Who has been notified already
        :return:dct_state to be written out
        '''
        self.dct_state['Cumulative_Consumption'] = df_cumulative_consumption.to_json()
        ls_notif = [{'Date' : str(rundate), 'Empty_Customers' : list(cust_notify_tdy)}]
        self.dct_state['daily_notifications'].extend(ls_notif)
        self.dct_state['cust_notified_already'] = list(cust_notified_already)
        self.dct_state['cust_notify_today'] = list(cust_notify_tdy)

    def error_consum(self, disp_weight_rel, df_cumulative_consumption, date1):
        '''
        Calculate the error in prediction for today
        :param disp_weight_rel: For the customers who has had their delivery last day
        :param df_cumulative_consumption: Cumulative consumption of gas so far from daily prediction since last delivery
        :param date1: Code executed date
        :return:
        '''
        disp_weight_rel = disp_weight_rel.rename({'DispensedWeight': 'Cumulative_Consumption'}, axis=1)
        df_3 = pd.DataFrame()
        df_3['pred_cons'] = (df_cumulative_consumption.loc[disp_weight_rel.index, 'Cumulative_Consumption'])
        df_3['disp_wt'] = (disp_weight_rel['Cumulative_Consumption'])
        df_3['Date'] = date1
        df_3['diff'] = (df_3['pred_cons'] - df_3['disp_wt']) * 100 / df_3['disp_wt']
        self.df_1 = self.df_1.append(df_3)

    def notif(self, date1, cust_ent_date, custids, mean_temp, df_order, dct_state, cons):
        '''

        :param date1: Date of the algorithm execution, this method is called once for each day
        :param cust_ent_date: Dataframe containing entry date for each customer
        :param custids: Customer IDs
        :param mean_temp: Average temperature of the region at peak hours (one average per day)
        :param df_order: Order Data
        :param dct_state: Dictionary containing state information like last_run_date, notified_customers, customers to be notified by date etc
        :param cons: Dataframe containing predicted consumption by day for each customer
        :return: cons and updated dct_state
        '''
        rundate = date1.date() # date of execution of this method
        last_run_date, cust_notified_already = self.read_last_run(dct_state) # Collect information from previous run

        # Estimate the daily consumption for date1 for all relevant customers
        pres_cons = cust_ent_date.loc[cust_ent_date['Join_Date'] <= pd.Timestamp(rundate)].index.tolist()
        rel_cust_run = [cust for cust in pres_cons if cust in custids] # Find the customers for whom the consumption need to be calculated - some customers might not have been part of OnGas by rundate.
        customer = Customers(rel_cust_run, self.df_coeffs, self.bins) # Instantiate customers class
        runday_cons = customer.daily_cons(mean_temp) # Calculate daily consumption
        cons.loc[runday_cons.index, rundate] = runday_cons # Append the calculated consumption to cons dataframe


        dct_cumulative_consumption = json.loads(dct_state['Cumulative_Consumption']) # Load cumulative consumption from state dictionary for each customer
        df_cumulative_consumption = pd.DataFrame(dct_cumulative_consumption) # Convert it to dataframe
        df_cumulative_consumption.index = df_cumulative_consumption.index.astype(int)  # JSON DUMPED to str, convert it to int

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
        cust_notify_tdy = cust_due_notif - set(cust_notified_already)  # Exclude already notified customers from previous runs
        cust_notified_already = set(cust_notified_already)
        cust_notified_already.update(cust_ready_to_order) # Update the notified customers list for writing it out into state after the run

        check = any(item in custids for item in disp_weight.index) and (disp_weight['DispensedWeight'].sum() > 0)

        if check: # If any customers have received deliveries yesterday, calculate error in prediction
            disp_weight_rel = pd.DataFrame(disp_weight.loc[disp_weight.index.isin(custids), 'DispensedWeight'])
            self.error_consum(disp_weight_rel, df_cumulative_consumption, rundate)

        cust_deliv_last_day = set(disp_weight.index) # Find who got their deliveries during last run
        cust_notified_already = (cust_notified_already - cust_deliv_last_day) # Edit the customers notified already section to ensure we remove those who received their cylinders

        if not (disp_weight.empty) and any(item in custids for item in cust_deliv_last_day): # Checkpoint to make sure the program doesnt hit an error
            disp_weight['Cumulative_Consumption'] = disp_weight.loc[disp_weight.index.isin(custids), :]
            df_cumulative_consumption.fillna(0, inplace=True)
            if not self.trial: # During non-trial, both cylinders wont be replaced. We need to make sure the predicted consumption if more than 45kg gets into second bottle
                # Adjust the cumulative consumption - take away the dispensed weight from cumulative consumption.
                df_cumulative_consumption.loc[
                    df_cumulative_consumption.index.isin(cust_deliv_last_day), 'Cumulative_Consumption'] = \
                    df_cumulative_consumption.loc[
                        df_cumulative_consumption.index.isin(cust_deliv_last_day), 'Cumulative_Consumption'] - \
                    disp_weight.loc[disp_weight.index.isin(cust_deliv_last_day), 'Cumulative_Consumption']
            elif self.trial: # During trial, both cylinders were replaced
                # Cumulation is reset to zero
                df_cumulative_consumption.loc[
                    df_cumulative_consumption.index.isin(cust_deliv_last_day), 'Cumulative_Consumption'] = 0

        df_cumulative_consumption['Cumulative_Consumption'] = df_cumulative_consumption[
            'Cumulative_Consumption'].clip(lower=0, upper=dbl_ord_qty) # If this value is negative, make it zero and if it is more than double cylinder quantity, limit it to two cylinder quantity

        # Write the outputs to files
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
