import os
import boto3
import pandas as pd
from io import StringIO
import json


class HandleData():
    def __init__(self, aws_id, aws_secret, bucket, startfromscratch, custids, startdate, run, mode='r'):
        '''
        This module read/ writes data according to different stages of the execution.
        It is programmed to consume this data from AWS or from local files
        :param aws_id: Placeholder for AWS UserID
        :param aws_secret: Placeholder for AWS password
        :param bucket: Placeholder for AWS bucket with data
        :param startfromscratch: bool, if the prediction would start from scratch or run from previous save point
        :param custids: list, Customer list
        :param startdate: DateString, Start date of run
        :param run: 1 deg interval or 2 deg interval run option.
        :param mode: Read/Write mode, controlled by program execution state
        '''
        self.client = boto3.client('s3', aws_access_key_id=aws_id,
                                   aws_secret_access_key=aws_secret)
        self.mode = mode # Switch between read mode and write mode
        self.startfromscratch = startfromscratch
        self.bucket = bucket
        self.run = run
        if self.startfromscratch: # If starting from scratch, set up dct_inp as there wont be any file to read.
            self.dct_inp = {}
            self.dct_inp['state'] = {}
            self.dct_inp['state']['last_run_date'] = pd.to_datetime(startdate, format='%d/%m/%Y') # Last run date is startdate
            self.dct_inp['state']['cust_notified_already'] = set() # Empty list for customers notified already
            self.dct_inp['state']['Cumulative_Consumption'] = pd.DataFrame(0, index=custids,
                                                                           columns=['Cumulative_Consumption']).to_json() # 0 consumption so far for all customers
            self.dct_inp['state']['daily_notifications'] = [] # EMpty list of daily notifications

    def __read_json(self, bucket_name, object_key):
        '''
        Read JSON from AWS
        :param bucket_name: Bucket name
        :param object_key: JSON object name
        :return: JSON content as object
        '''
        content_object = self.client.get_object(bucket_name, object_key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        return json_content

    def __read_csv(self, bucket_name, object_key):
        '''
        Read csv from AWS
        :param bucket_name: Bucket name
        :param object_key: CSV object name
        :return: CSV content as dataframe
        '''
        csv_obj = self.client.get_object(Bucket=bucket_name, Key=object_key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        return df

    def __write_json(self, bucket_name, object_key, file):
        '''
        Write JSON into AWS
        :param bucket_name: Bucket name
        :param object_key: JSON object name
        :param file: File to write the JSON object
        :return:
        '''
        serializedMyData = json.dumps(file)
        self.client.put_object(Bucket=bucket_name, Key=object_key, Body=serializedMyData)

    def __write_csv(self, bucket_name, object_key, file):
        '''
        Write CSV object into AWS
        :param bucket_name: Bucket name
        :param object_key: CSV object name
        :param file: File to write the CSV object
        :return:
        '''
        csv_buffer = StringIO()
        file.to_csv(csv_buffer)
        self.client.put_object(bucket_name, key=object_key, Body=csv_buffer.getvalue())

    def _handle_file_rw(self, bucket, file_loc, filetype, file=None):
        '''
        This module orchestrates read and write operations
        :param bucket: AWS bucket location placeholder
        :param file_loc: Location of file in the bucket
        :param filetype: Type of file, csv or JSON
        :param file: Filename
        :return: dataframe of the file that is being read if in read mode else None
        '''
        if self.mode == 'r':
            if filetype == 'csv':
                df = self.__read_csv(bucket, file_loc)
            elif filetype == 'json':
                df = self.__read_json(bucket, file_loc)
            return df
        else:
            if filetype == 'csv':
                self.__write_csv(bucket, file_loc, file)
            elif filetype == 'json':
                self.__write_json(bucket, file_loc, file)

    def read_write_main(self, state=None, cons=None):
        '''
        This module is for execution from AWS
        :param state:
        :param cons:
        :return:
        '''
        if self.mode == 'r':
            self.dct_inp['coeff'] = self._handle_file_rw(self.bucket, coeff_key, 'csv')
            if not (self.startfromscratch):
                self.dct_inp['state'] = self._handle_file_rw(self.bucket, state_key, 'json')
            self.dct_inp['order'] = self._handle_file_rw(self.bucket, order_key, 'csv')
            self.dct_inp['cust'] = self._handle_file_rw(self.bucket, cust_key, 'csv')
            self.dct_inp['temp'] = self._handle_file_rw(self.bucket, temp_key, 'csv')
            return self.dct_inp
        elif self.mode == 'w':
            self._handle_file_rw(self.bucket, state_key, 'json', state)
            self._handle_file_rw(self.bucket, cons_key, 'csv', cons)

    def read_write_main_local(self, state=None, cons=None):
        '''
        :param state: State file name
        :param cons: Consumption data file name
        :return:
                For read mode:
                    dct_inp - dictionary containing coefficients, order information, customer starting dates and temperature
                For write mode:
                    None
        '''
        coeff_file = r'./Data/Input/' + self.run +  '/Cust_coeff.csv'
        cons_file = r'./Data/Debug/' + self.run + '/cons.csv'
        notify_file = r'./Data/State/' + self.run + '/notify.json'

        if self.mode == 'r': # For read mode

            if not (self.startfromscratch): # If starting from scratch dct_inp would have been setup already
                self.dct_inp = {}
                with open(notify_file, 'r') as fr: # Read all the necessary information from last run
                    self.dct_inp['state'] = json.load(fr)

            self.dct_inp['coeff'] = pd.read_csv(coeff_file) # Add to it the coefficients
            self.dct_inp['order'] = pd.read_csv(r'./Data/Input/OrderData1.csv') # Add to it the order data

            try:
                self.dct_inp['cust'] = pd.read_csv(r'./Data/Input/Customer_First_Entry_Date.csv', header=None) # If the customer first entry date file is available
            except FileNotFoundError: #If not found create it from order data
                df_order = pd.read_csv(r'./Data/Input/OrderData1.csv')
                cust_first_ent = df_order.groupby(['AccountNumber']).min()['OrderDateKey']
                cust_first_ent.to_csv(r'./Data/Input/Customer_First_Entry_Date.csv', header=None)
                self.dct_inp['cust'] = pd.read_csv(r'./Data/Input/Customer_First_Enry_Date.csv', header=None)

            self.dct_inp['temp'] = pd.read_csv(r'./Data/Input/temperature1.csv') # Read the temperature data into the 'temp' key
            return self.dct_inp

        elif self.mode == 'w': # In write mode
            with open(notify_file, 'w') as fw: # Dump the state info into the notification file
                json.dump(state, fw, indent=4)

            cons.index.name = 'Customers'
            cons.columns.name = 'Date'
            cons = cons.T

            if os.path.isfile(cons_file): # If the cons_file exists
                df_cons = pd.read_csv(cons_file) # Read it
                df_cons['Date'] = pd.to_datetime(df_cons['Date'], format='%Y-%m-%d') # Convert the date string to datetime object
                df_cons.set_index('Date', inplace=True) # Set it as index
                df_cons.columns = [int(col) for col in df_cons.columns] # convert the columns to integers
                df_cons = df_cons.append(cons) # Append the consumption to existing file
                df_cons = df_cons[~df_cons.index.duplicated(keep='last')] # If there are duplicates, keep the last one
                df_cons.sort_index(inplace=True) # Sort the df by date
                df_cons.to_csv(cons_file) # write it out
            else: # If the file does not exist, write the consumption as file.
                cons.to_csv(cons_file)
