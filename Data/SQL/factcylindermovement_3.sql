/****** Script for SelectTopNRows command from SSMS  ******/
SELECT SUM(fcm.DispensedWeightAmount) as DispensedWeight
	,fcm.DeliveryCustomerAccountKey
	,dcc.AccountNumber
	,fcm.OrderDateKey
    ,op.PriorityStatus
	,fcm.MovementTypeKey
	,fcm.MovementDateKey
	,fcm.OnGasProductKey AS OnGasProductKey
	,fcm.OrderLineKey
	,fcm.CylinderKey
	,dcc.OnGasCustomerStatus
  FROM [NDS].[ECM].[factCylinderMovement] fcm
--  LEFT OUTER JOIN ECM.vdimDeliveryDepot dd on dd.DeliveryDepotKey = fcm.DeliveryDepotKey
  JOIN ECM.vdimOrderProfile op on fcm.OrderProfileKey = op.OrderProfileKey
  LEFT OUTER JOIN ECM.vdimDeliveryCustomerAccount dcc on fcm.DeliveryCustomerAccountKey = dcc.DeliveryCustomerAccountKey
  LEFT OUTER JOIN ECM.dimOnGasProduct ogp on fcm.OnGasProductKey = ogp.OnGasProductKey
  JOIN  (Select distinct p.DeliveryProfileKey from ECM.dimDeliveryProfile p where p.DeliveryProfileKey = 0 or p.DeliveryTruckId is not null)S
 on fcm.DeliveryProfileKey = S.DeliveryProfileKey 
  WHERE fcm.DeliveryDateKey <> 0 AND fcm.DeliveryCustomerAccountKey <> 0 and fcm.MovementTypeKey in (10, 6, 16) and dcc.OnGasCustomerStatus ='Active'and op.PriorityStatus <> 'Standing Order'
  AND ISNULL(dcc.AccountNumber,'') IN ('992037096',
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
'992889140', NULL) --AND fcm.OrderDateKey > 20190801
  GROUP BY fcm.DeliveryCustomerAccountKey, fcm.OrderDateKey, fcm.DeliveryTimeKey, op.PriorityStatus,fcm.MovementTypeKey, fcm.CylinderKey, fcm.OnGasProductKey
	,fcm.MovementDateKey
--	,dd.DeliveryDepotName
	,fcm.DeliveryDriverEmployeeKey
	,ogp.CfgModelName
	,fcm.OrderLineKey
	,fcm.CylinderKey
	,dcc.AccountNumber
	,dcc.OnGasCustomerStatus
  order by fcm.DeliveryCustomerAccountKey, fcm.OrderDateKey