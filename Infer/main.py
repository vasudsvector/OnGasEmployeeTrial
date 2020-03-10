import pandas as pd
import read_filter as rf
from Infer.notif import FuncNotif
from Infer.handle_data import HandleData
from datetime import datetime, timedelta
from errorpred import  ErrorPred


class RunPred():
    '''
    The class is the main class for running prediction algorithm
    '''
    def __init__(self, aws_id, aws_secret, bucket, startfromscratch, startdate, run_until_date=None, custids=None,
                 local_test=False, employeetrial=False, bins=range(5, 25, 3), run='1_simp_avg'):
        '''
        :param aws_id: Placeholder for running in AWS,ID of the user account
        :param aws_secret: Placeholder for running in AWS, password of the user account
        :param bucket: Placeholder for bucket containing data in AWS S3
        :param startfromscratch: bool, specifies if the data analysis needs to be started from scratch (from startdate) or from last run
        :param startdate: Date string, '%d/%m/%Y'. start date from which the analysis needs to be performed. Useful only if startfromscratch is True
        :param run_until_date: Date string, '%d/%m/%Y. end date for the run.
        :param custids:list, string containing customer account IDs for which analysis need to be performed
        :param local_test:bool, if local_test = True, it access files from local directory, else it expects AWS details
        :param employeetrial: The cumulation of consumption is different for trial and production. During trial since we are replacing both cylinders, both cylinders start fresh. During actual
                                production run, only one cylinder will be replaced. If the prediction is larger than dispensed amount, the consumption might have come from second cylinder.
                                The second cylinder will be deducted with difference.
        :param bins: tuple containing (min temp, max temp, bin interval)
        :param run: Run option. Can be removed for production. Used to test which model was most effective. Explanation of different run types is available in readme.md
        '''
        self.aws_id = aws_id
        self.aws_secret = aws_secret
        self.startfromscratch = startfromscratch
        self.bucket = bucket
        self.startdate = startdate
        self.employeetrial = employeetrial
        self.run = run

        # Control of bin size during trial was here
        if '1_' in self.run: # For 1 degree interval bins
            self.bins = range(2,25,1)
        elif '2_' in self.run: # For 2 degree interval bins
            self.bins = range(2, 25, 2)

        if run_until_date is None: # set todays date as last day to run if not provided
            self.run_until_date = datetime.today() - timedelta(1)
        else:
            self.run_until_date = run_until_date

        if custids == None: # Placeholder to set customer list from order history if not provided
            self.custids = self.extract_cust()  # TODO write this function to extract cust list from order history
        else: # convert the customer list into integers
            custids1 = [int(cust) for cust in custids]
            self.custids = custids1

        if local_test: # setup the aws key for choosing between local and aws location for the run
            self.aws = False
        else:
            self.aws = True

    def read_data(self):
        '''
        Read data from all sources
        :return: dct_inp
        '''
        hd = HandleData(self.aws_id, self.aws_secret, self.bucket, self.startfromscratch, self.custids, self.startdate, self.run,
                        mode='r')
        if self.aws:
            dct_inp = hd.read_write_main()
        else:
            dct_inp = hd.read_write_main_local()
        return dct_inp

    def write_data(self, lat_cons, dct_state):
        '''
        Write the data generated for next run or for postprocessing
        :return: None
        '''
        hd = HandleData(self.aws_id, self.aws_secret, self.bucket, self.startfromscratch, self.custids, self.startdate,self.run,
                        mode='w')
        if self.aws:
            hd.read_write_main(dct_state, lat_cons)
        else:
            hd.read_write_main_local(dct_state, lat_cons)

    def main(self):
        '''
        This is where the action starts and ends. This module calls all the methods necessary for execution
        :return: None
        '''
        dct_inp = self.read_data()
        last_run_date = dct_inp['state']['last_run_date']
        run_date = self.run_until_date
        df_coeff = dct_inp['coeff']
        dct_state = dct_inp['state']
        df_order = dct_inp['order']
        cust_ent_date = dct_inp['cust']
        df_temp = dct_inp['temp']

        fn = FuncNotif(self.custids, last_run_date, run_date, coeff=df_coeff, dct_state=dct_state,
                       employeetrial=self.employeetrial, bins=self.bins)

        df_order = rf.filter_data(df_order)

        # Clean and Format Customer start date
        cust_ent_date.columns = ['Customer', 'Join_Date']
        cust_ent_date.set_index('Customer', inplace=True)
        cust_ent_date['Join_Date'] = pd.to_datetime(cust_ent_date['Join_Date'], format='%Y%m%d')

        # Include customer whose joining date is before the run dates
        custlist1 = cust_ent_date.loc[cust_ent_date['Join_Date'] <= fn.rundates[-1]].index.tolist()
        custlist = [cust for cust in custlist1 if cust in self.custids]
        lat_cons = pd.DataFrame(None, index=custlist,
                                columns=fn.rundates)  # Init df - df containing daily consumption for all dates in the current run

        df_temp_rel = fn.fetch_temp(df_temp) # Calculates the average temperature for the day and puts it in time series

        for date1 in fn.rundates:  # Run it for all days between previous run and current run
            mean_temp = df_temp_rel.loc[
                date1.date(), 'Mean_Temp']  # Mean temperature of the day, calculated during peak hours

            if date1.strftime('%Y-%m-%d') not in dct_state: # Is going to execute for all dates in the run period, if it is not found in already executed file
                lat_cons, dct_state = fn.notif(date1=date1, cust_ent_date=cust_ent_date, custids=self.custids,
                                               mean_temp=mean_temp, df_order=df_order,
                                               dct_state=dct_state, cons=lat_cons)

            dct_state['last_run_date'] = str(date1.date()) # Overwrite last_run_date at the end of each loop, makes sure even if crashes midway the last_run_date is captured and we can start from there

        self.write_data(lat_cons, dct_state)

        ep = ErrorPred(df_order, lat_cons, fn.rundates)
        df_ord = ep.filter_orders_by_date() # Filter the orders that belong to rundates
        df_consum = ep.calc_act_consump(df_ord) # Calculate actual consumption for each customer by movement date
        df_dates = df_consum.apply(lambda x: ep.get_last_order_date(x['DeliveryCustomerAccountKey'], x['MovementDateKey']), axis=1) # Find order date for each delivery
        df_consum['last_order_date'] = df_dates
        df_consum.set_index('DeliveryCustomerAccountKey', inplace=True)
        df_consum = df_consum.loc[df_consum['MovementDateKey'] >= pd.to_datetime('08/01/2019'), :] # This is filtered for trial purposes to handle small data instead of large data
        pred_cons = ep.get_pred_cons(df_consum) # Obtain the predicted consumption from cumulative consumption dataframe
        df_error = df_consum.join(pred_cons, rsuffix='_pred')
        df_error['Error'] = (df_error['DispensedWeight'] - df_error[0]) * 100 / df_error['DispensedWeight']
        df_error.to_csv('./Data/Debug/' + self.run + '/Error.csv')
        # dct_state = fn.read_state(state_loc)


if __name__ == '__main__':
    custids = ['992037096',
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
    for run in ['1_simp_avg', '1_smoothed', '2_simp_avg', '2_smoothed']:
        rp = RunPred('aws_id', 'aws_sec', 'aws_buck', startdate='10/07/2019', startfromscratch=True,
                 run_until_date='07/03/2020', custids=custids, local_test=True, employeetrial=True,
                 bins=range(2, 25, 1), run=run)
        rp.main()

    ### Define Variables ###
    ## File Locations ##

    # Inputs
    # coeff_loc = r'./Data/Customer_Coefficients.csv'
    # folder = r'./Data/'
    # filename = r'CustomerOrders-activecustomers-normalorders-04Apr2019.csv'
    # custentdatefile = r'./Data/Customer_First_Entry_Date.csv'
    # temp_file = r'./Data/temperature.csv'
    #
    # # Input and Output
    # state_loc = r'./Data/state_file.csv'
    # cons_file = r'./Data/calc_consumption.csv'
    # notify_file = r'./Data/notify.json'
    #
    # order_loc = os.path.join(folder, filename)
    #
    # df_order = rf.read_csv(order_loc)
    # df_temp = pd.read_csv(temp_file)
    #
    # custids = [184402.0, 303224.0, 315103.0, 372171.0, 373548.0, 504768.0,
    #            1048392.0, 1240691.0, 1592741.0, 1681144.0, 1760507.0, 1798780.0,
    #            1836847.0, 2430166.0, 2848345.0, 2881646.0, 2896783.0, 2928320.0,
    #            3027296.0, 3123743.0, 3148573.0]
    # last_run_date = '2015/06/30'
    # run_date = '2016/07/31'
    #
    # df_state = pd.DataFrame(0, index=custids, columns=['Cumulative_Consumption'])
    # df_state.to_csv(state_loc)
