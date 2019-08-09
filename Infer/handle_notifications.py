import pandas as pd
from datetime import timedelta


class Order_Msg():
    def __init__(self, run_date, last_run_date, ord_qty, dbl_ord_qty):
        self.run_date = run_date
        self.last_run_date = last_run_date
        if ord_qty or dbl_ord_qty is None:
            self.ord_qty = 43
            self.dbl_ord_qty = 86
        else:
            self.ord_qty = ord_qty
            self.dbl_ord_qty = dbl_ord_qty

    def find_latest_orders(self, df_auck_sub):
        df_latest_order = df_auck_sub.loc[df_auck_sub['OrderDateKey'] == (self.run_date) - timedelta(1),:]#.between(self.last_run_date, self.run_date)]
        orders_last_day = df_latest_order.loc[:,['DeliveryCustomerAccountKey','DispensedWeight']]
        return orders_last_day

    def find_latest_deliveries(self, df_auck_sub):
        df_latest_delivery = df_auck_sub.loc[df_auck_sub['MovementDateKey'] == (self.run_date) - timedelta(1), :]#.between(self.last_run_date, self.run_date)]
        deliveries_last_day = df_latest_delivery.loc[:,['DeliveryCustomerAccountKey','DispensedWeight']]
        return deliveries_last_day

    def categorise_customers(self, df_state):
        cust_ready_to_order = df_state.loc[df_state['Cumulative_Consumption'].between(self.ord_qty, self.dbl_ord_qty+0.1),
                              :].index.tolist()
        cust_emptied_both = df_state.loc[df_state['Cumulative_Consumption'] >= self.dbl_ord_qty, :].index.tolist()
        return set(cust_ready_to_order), set(cust_emptied_both)

    def remove_already_ordered(self, df_auck_sub, cust_ready_to_order, cust_emptied_both):
        cust_ready_to_order.update(cust_emptied_both)
        orders_last_day = self.find_latest_orders(df_auck_sub)
        cust_ordered_last_day = set(orders_last_day['DeliveryCustomerAccountKey'])
        cust_due_notif = set(cust_ready_to_order) - set(cust_ordered_last_day)

        deliv_last_day = self.find_latest_deliveries(df_auck_sub)
        disp_weight = deliv_last_day.groupby('DeliveryCustomerAccountKey').sum()
        return cust_due_notif, cust_ordered_last_day, disp_weight