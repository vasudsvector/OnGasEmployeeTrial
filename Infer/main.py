import pandas as pd
import read_filter as rf
from Infer.notif import FuncNotif
from Infer.handle_data import HandleData
from datetime import datetime, timedelta


class RunPred():
    def __init__(self, aws_id, aws_secret, bucket, startfromscratch, startdate, run_until_date=None, custids=None,
                 local_test=False, employeetrial=False, bins=range(5, 25, 3), run='1_simp_avg'):
        self.aws_id = aws_id
        self.aws_secret = aws_secret
        self.startfromscratch = startfromscratch
        self.bucket = bucket
        self.startdate = startdate
        self.employeetrial = employeetrial
        self.run = run
        if '1_' in self.run:
            self.bins = range(2,25,1)
        elif '2_' in self.run:
            self.bins = range(2, 25, 2)

        if run_until_date is None:
            self.run_until_date = datetime.today() - timedelta(1)
        else:
            self.run_until_date = run_until_date

        if custids == None:
            self.custids = self.extract_cust()  # TODO write this function to extract cust list from order history
        else:
            custids1 = [int(cust) for cust in custids]
            self.custids = custids1

        if local_test:
            self.aws = False
        else:
            self.aws = True

    def read_data(self):
        hd = HandleData(self.aws_id, self.aws_secret, self.bucket, self.startfromscratch, self.custids, self.startdate, self.run,
                        mode='r')
        if self.aws:
            dct_inp = hd.read_write_main()
        else:
            dct_inp = hd.read_write_main_local()
        return dct_inp

    def write_data(self, lat_cons, dct_state):
        hd = HandleData(self.aws_id, self.aws_secret, self.bucket, self.startfromscratch, self.custids, self.startdate,self.run,
                        mode='w')
        if self.aws:
            hd.read_write_main(dct_state, lat_cons)
        else:
            hd.read_write_main_local(dct_state, lat_cons)

    def main(self):
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

        df_order = rf.filter_data(df_order)  # TODO update this after trial to include other areas

        cust_ent_date.columns = ['Customer', 'Join_Date']
        cust_ent_date.set_index('Customer', inplace=True)
        cust_ent_date['Join_Date'] = pd.to_datetime(cust_ent_date['Join_Date'], format='%Y%m%d')

        custlist1 = cust_ent_date.loc[cust_ent_date['Join_Date'] <= fn.rundates[-1]].index.tolist()
        custlist = [cust for cust in custlist1 if cust in self.custids]
        lat_cons = pd.DataFrame(None, index=custlist,
                                columns=fn.rundates)  # Init df - df containing daily consumption for all dates in the current run

        df_temp_rel = fn.fetch_temp(df_temp)
        for date1 in fn.rundates:  # Run it for all days between previous run and current run
            mean_temp = df_temp_rel.loc[
                date1.date(), 'Mean_Temp']  # Mean temperature of the day, calculated during peak hours
            if date1.strftime('%Y-%m-%d') not in dct_state:
                lat_cons, dct_state = fn.notif(date1=date1, cust_ent_date=cust_ent_date, custids=self.custids,
                                               mean_temp=mean_temp, df_order=df_order,
                                               dct_state=dct_state, cons=lat_cons)
            dct_state['last_run_date'] = str(date1.date())
        self.write_data(lat_cons, dct_state)

        df2 = fn.df_1
        df2.to_csv('./Data/Debug/' + self.run + '/Error.csv')
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
                 run_until_date='23/08/2019', custids=custids, local_test=True, employeetrial=True,
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
