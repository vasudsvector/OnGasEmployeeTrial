import pandas as pd
from datetime import timedelta


class Order_Msg():
    '''
    This class is meant to handle information about notifications
    '''
    def __init__(self, run_date, last_run_date, ord_qty, dbl_ord_qty):
        '''
        :param run_date: Date of code execution
        :param last_run_date: When was the code last executed
        :param ord_qty: How much is typical cylinder size - When the algorithm needs to raise notification
        :param dbl_ord_qty: How much is two cylinder size - When the algorithm needs to stop accumulating
        '''
        self.run_date = run_date
        self.last_run_date = last_run_date
        if ord_qty or dbl_ord_qty is None: # If there no values declared, assume as below
            self.ord_qty = 44.5
            self.dbl_ord_qty = 90
        else: # Assign values provided while instatiating
            self.ord_qty = ord_qty
            self.dbl_ord_qty = dbl_ord_qty

    def find_latest_orders(self, df_auck_sub):
        '''
        :param df_auck_sub: Dataframe with latest order information
        :return: Orders from previous day - Exclude them for notifications as they have already ordered
        '''
        df_latest_order = df_auck_sub.loc[df_auck_sub['OrderDateKey'] == (self.run_date) - timedelta(1),:]#.between(self.last_run_date, self.run_date)]
        orders_last_day = df_latest_order.loc[:,['DeliveryCustomerAccountKey','DispensedWeight']]
        return orders_last_day

    def find_latest_deliveries(self, df_auck_sub):
        '''
        Find the people who have received their deliveries during last day. This is to reset their consumption or stock levels
        :param df_auck_sub: Dataframe with latest order information
        :return: Deliveries from previous day
        '''
        df_latest_delivery = df_auck_sub.loc[df_auck_sub['MovementDateKey'] == (self.run_date) - timedelta(1), :]#.between(self.last_run_date, self.run_date)]
        deliveries_last_day = df_latest_delivery.loc[:,['DeliveryCustomerAccountKey','DispensedWeight']]
        return deliveries_last_day

    def categorise_customers(self, df_state):
        '''
        categorise the customers who has emptied one cylinder and customers who emptied both according to prediction
        :param df_state:
        :return:
        '''
        cust_ready_to_order = df_state.loc[df_state['Cumulative_Consumption'].between(self.ord_qty, self.dbl_ord_qty+0.1),
                              :].index.tolist()
        cust_emptied_both = df_state.loc[df_state['Cumulative_Consumption'] >= self.dbl_ord_qty, :].index.tolist()
        return set(cust_ready_to_order), set(cust_emptied_both)

    def remove_already_ordered(self, df_auck_sub, cust_ready_to_order, cust_emptied_both):
        '''
        :param df_auck_sub: Latest order information
        :param cust_ready_to_order: Customers who are predicted to have emptied their bottle
        :param cust_emptied_both: Customers who are predicted to have emptied both their bottles
        :return:
        cust_due_notif: Customer list who are due to be notified today
        cust_ordered_last_day: Customers who ordered during previous day
        disp_weight: Dispensed weight by customer for previous day deliveries
        '''
        cust_ready_to_order.update(cust_emptied_both)
        orders_last_day = self.find_latest_orders(df_auck_sub)
        cust_ordered_last_day = set(orders_last_day['DeliveryCustomerAccountKey'])
        cust_due_notif = set(cust_ready_to_order) - set(cust_ordered_last_day) # Remove the customers who have already, they dont need to be notified

        deliv_last_day = self.find_latest_deliveries(df_auck_sub) # Deliveries from previous day
        disp_weight = deliv_last_day.groupby('DeliveryCustomerAccountKey').sum() # Dispensed weight by customer
        return cust_due_notif, cust_ordered_last_day, disp_weight