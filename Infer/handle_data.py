import os
import boto3
import pandas as pd
from io import StringIO
import json


class HandleData():
    def __init__(self, aws_id, aws_secret, bucket, startfromscratch, custids, startdate, mode='r'):
        self.client = boto3.client('s3', aws_access_key_id=aws_id,
                              aws_secret_access_key=aws_secret)
        self.mode = mode
        self.startfromscratch = startfromscratch
        self.bucket = bucket
        if self.startfromscratch:
            self.dct_inp = {}
            self.dct_inp['state'] = {}
            self.dct_inp['state']['last_run_date'] = pd.to_datetime(startdate, format='%d/%m/%Y')
            self.dct_inp['state']['cust_notified_already'] = set()
            self.dct_inp['state']['Cumulative_Consumption'] = pd.DataFrame(0, index=custids, columns=['Cumulative_Consumption']).to_json()

    def __read_json(self, bucket_name, object_key):
        content_object = self.client.get_object(bucket_name, object_key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        return json_content

    def __read_csv(self, bucket_name, object_key):
        csv_obj = self.client.get_object(Bucket=bucket_name, Key=object_key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        return df

    def __write_json(self, bucket_name, object_key, file):
        serializedMyData = json.dumps(file)
        self.client.put_object(Bucket=bucket_name, Key=object_key, Body = serializedMyData)

    def __write_csv(self, bucket_name, object_key, file):
        csv_buffer = StringIO()
        file.to_csv(csv_buffer)
        self.client.put_object(bucket_name, key=object_key, Body=csv_buffer.getvalue())

    def _handle_file_rw(self, bucket, file_loc, filetype, file=None):
        if self.mode == 'r':
            if filetype == 'csv':
                df = self.__read_csv(bucket, file_loc)
            elif filetype == 'json':
                df = self.__read_csv(bucket, file_loc)
            return df
        else:
            if filetype == 'csv':
                self.__write_csv(bucket, file_loc, file)
            elif filetype == 'json':
                self.__write_json(bucket, file_loc, file)

    def read_write_main(self, state=None, cons=None):
        if self.mode == 'r':
            self.dct_inp['coeff'] = self._handle_file_rw(self.bucket, coeff_key, 'csv')
            if not(self.startfromscratch):
                self.dct_inp['state'] = self._handle_file_rw(self.bucket, state_key, 'json')
            self.dct_inp['order'] = self._handle_file_rw(self.bucket, order_key, 'csv')
            self.dct_inp['cons'] = self._handle_file_rw(self.bucket, cons_key, 'csv')
            self.dct_inp['cust'] = self._handle_file_rw(self.bucket, cust_key, 'csv')
            self.dct_inp['temp'] = self._handle_file_rw(self.bucket, temp_key, 'csv')
            return self.dct_inp
        elif self.mode == 'w':
            self._handle_file_rw(self.bucket, state_key, 'json', state)
            self._handle_file_rw(self.bucket, cons_key, 'csv', cons)


    def read_write_main_local(self, state=None, cons=None):
        if self.mode == 'r':
            if not(self.startfromscratch):
                self.dct_inp = {}
                with open(r'./data/notify_simp_avg.json', 'r') as fr:
                    self.dct_inp['state'] = json.load(fr)
                self.dct_inp['cons'] = pd.read_csv(r'./data/OrderData1.csv') #TODO Reevaluate the need for this
            self.dct_inp['coeff'] = pd.read_csv(r'./Cust_coeff_simple_avg.csv')
            self.dct_inp['order'] = pd.read_csv(r'./data/OrderData1.csv')
            try:
                self.dct_inp['cust'] = pd.read_csv(r'./data/Customer_First_Entry_Date.csv', header=None)
            except FileNotFoundError:
                df_order = pd.read_csv(r'./data/OrderData1.csv')
                cust_first_ent = df_order.groupby(['DeliveryCustomerAccountKey']).min()['OrderDateKey']
                cust_first_ent.to_csv(r'./data/Customer_First_Entry_Date.csv')
                self.dct_inp['cust'] = pd.read_csv(r'./data/Customer_First_Enry_Date.csv', header=None)
            self.dct_inp['temp'] = pd.read_csv(r'./data/temperature1.csv')
            return self.dct_inp
        elif self.mode == 'w':
            with open(r'./data/notify_simp_avg.json', 'w') as fw:
                json.dump(state, fw, indent=4)
            cons.to_csv(r'./data/cons.csv')