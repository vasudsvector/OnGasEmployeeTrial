import pandas as pd
import os
import proc_temperature as pt
import Train.redist_dbl_ord as ro
import Train.split_by_season as ss
import Train.crt_ts as ct
import read_filter as rf

def _calc_day_to_order(df):
    '''
    Feature creation by customer
    :param df: Dataframe containing order information
    :return:
    '''
    df['MovementDateKey_S'] = df['MovementDateKey'].shift(1)
    df['consumption_days'] = (df['MovementDateKey'] - df['MovementDateKey_S'])
    df['days_to_order'] = (df['OrderDateKey'] - df['MovementDateKey_S'])
    df['days_to_order'] = [day.days for day in df['days_to_order']]
    df['consumption_days'] = [day.days for day in df['consumption_days']]
    df['order_num'] = [i for i in range(df.shape[0])]
    df['DispensedWeight'] = df.groupby(['OrderDateKey', 'MovementDateKey'])['DispensedWeight'].transform('sum')
    dupl = (df.duplicated(['OrderDateKey', 'MovementDateKey']))
    df = df.loc[~(dupl)]

    # df = df.iloc[:-1, :]
    return df

def create_feat(df_auck_sub):
    '''
    Create Features for exploratory data analysis - Day of week, Month, Day of month etc.
    The function also combines double orders placed on single day
    :param df_auck_sub: Subsetted order data
    :return: Features for each order
    '''
    df_dto = df_auck_sub.groupby(['DeliveryCustomerAccountKey']).apply(lambda x: _calc_day_to_order(x))
    return df_dto

if __name__ == '__main__':
    folder = r'C:\Users\SurendranV\Projects\OnGas\Employeetrial\Data\\'
    filename = r'OrderData1.csv'
    file = os.path.join(folder, filename)
    df = rf.read_csv(file)
    df_auck_sub = rf.filter_data(df)

    #TODO Remove following two rows
    #np.random.seed(1)
    rand_50_cus = ['30542','1175528','1209153','1243555','1492996','1809945','2374423','2507659','2573868','2823654','2904434','3023747','3043106','3158828','3160294','3160605','3166687','3236446','3380997']
                    #pd.unique(df_auck_sub['DeliveryCustomerAccountKey'])[np.random.randint(0, 3034, 50)] #Total 3034 customers in the lot
    df_auck_sub = df_auck_sub.loc[df_auck_sub['DeliveryCustomerAccountKey'].isin(rand_50_cus),:]
    ###############################

    df_dto = create_feat(df_auck_sub)
    temp_cust = ss.seas_split(df_dto)
    ls_dbl_redist = ro.redist_dbl_order(df_dto.drop('DeliveryCustomerAccountKey', 1).reset_index())
    df_dbl_redist = pd.concat(ls_dbl_redist)
    temp_file = r'./Data/temperature.csv'
    df_temp = pd.read_csv(temp_file)
    df_temp = pt.proc_temperature(df_temp)
    df_ts = ct.calc_ts_feat(df_dbl_redist, df_temp)