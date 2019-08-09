import pandas as pd

def handle_residual(df):
    df.MovementTypeKey = df.MovementTypeKey.replace({6:"Delivery",10:"Fill",16:"PickUp"})
    df.loc[df['OnGasProductKey'] == 69,'MovementTypeKey'] = 'Residual'
    df_p = df.loc[df['MovementTypeKey']=='PickUp',:]
    df_r = df.loc[df['OnGasProductKey'] == 69,:]
    df_p['MovementDateKey'] = pd.to_datetime(df_p['MovementDateKey'], format='%Y%m%d')
    df_r['MovementDateKey'] = pd.to_datetime(df_r['MovementDateKey'], format='%Y%m%d')
    df_r.set_index(['CylinderKey','MovementDateKey'],inplace=True)
    df_p.set_index(['CylinderKey','MovementDateKey'],inplace=True)
    df_r = df_r.sort_index(level=1)
    df_p = df_p.sort_index(level=1)
    df_r1 = pd.merge_asof(df_r, df_p, on=['MovementDateKey'], by='CylinderKey', direction='backward', tolerance=pd.Timedelta('5D'))
    df_r2 = df_r1.drop(['DeliveryCustomerAccountKey_x','OrderDateKey_x','PriorityStatus_x',
                        'DispensedWeight_y','MovementTypeKey_y','OnGasProductKey_y','OrderLineKey_x',
                        'AccountNumber_y', 'OnGasCustomerStatus_y'],1)
    ls1 = df_r2.columns
    col = [s.split('_')[0] if len(s.split('_'))>1 else s for s in ls1]
    df_r2.columns = col
    df = df.append(df_r2)
    return df

def assign_pickup_dates(df):
    df_p = df.loc[df['MovementTypeKey']=='PickUp',:]
    df_f = df.loc[(df['MovementTypeKey'] == 'Fill') | (df['MovementTypeKey'] == 'Residual'), :]
    df_p1 = df_p.set_index(['OrderLineKey', 'CylinderKey'])
    df_f1 = df_f.set_index(['OrderLineKey','CylinderKey'])
    df_p2 = df_p1.join(df_f1.loc[:,['DispensedWeight','MovementDateKey', 'MovementTypeKey']], rsuffix='_r')
    df_p3 = df_p2.sort_values(['DeliveryCustomerAccountKey','MovementDateKey'])
    df_p3['DispensedWeight'] = df_p3['DispensedWeight_r']
    df_p3['MovementTypeKey'] = df_p3['MovementTypeKey_r']
    df_p3 = df_p3.drop(['DispensedWeight_r', 'MovementDateKey_r', 'MovementTypeKey_r'], axis=1)
    df_p3.reset_index(inplace=True)
    return df_p3


if __name__ == '__main__':
    df = pd.read_csv(r'C:\Users\SurendranV\Projects\OnGas\Employeetrial\Data\OrderData1.csv')
    df = handle_residual(df)
    df = assign_pickup_dates(df)