-- Extract Metservice weather data and aggregate by specified time periods.

WITH 

TimeDimension AS 
(
SELECT DISTINCT
 f.MeasurementDateKey DateKey
 ,f.MeasurementTime TimeKey
FROM NDS.Weather.factMetserviceWeatherMeasurement f
),

TemperatureData AS
(
SELECT
 f.MeasurementDateKey
 ,f.MeasurementTime
 ,AVG(CONVERT(FLOAT,f.MeasurementValue)) AS Temperature  --deg C
FROM NDS.Weather.factMetserviceWeatherMeasurement f
JOIN NDS.Weather.dimMetserviceElement e ON e.MetserviceElementKey = f.MetserviceElementKey
JOIN NDS.Weather.dimMetserviceLocation l ON l.MetserviceLocationKey = f.MetserviceLocationKey
WHERE
MeasurementElementType IN ('Observation')
AND e.MeasurementElementName IN ('Temperature')
AND l.MeasurementLocationName IN ('AUCKLAND','HARBOUR BRIDGE','WHANGAPARAOA','WHENUAPAI')
GROUP BY f.MeasurementDateKey
		  ,f.MeasurementTime
),

DewpointData AS
(
SELECT
 f.MeasurementDateKey
 ,f.MeasurementTime
 ,AVG(CONVERT(FLOAT,f.MeasurementValue)) AS Dewpoint  --deg C
FROM NDS.Weather.factMetserviceWeatherMeasurement f
JOIN NDS.Weather.dimMetserviceElement e ON e.MetserviceElementKey = f.MetserviceElementKey
JOIN NDS.Weather.dimMetserviceLocation l ON l.MetserviceLocationKey = f.MetserviceLocationKey
WHERE
MeasurementElementType IN ('Observation')
AND e.MeasurementElementName IN ('Dewpoint')
AND l.MeasurementLocationName IN ('AUCKLAND','HARBOUR BRIDGE','WHANGAPARAOA','WHENUAPAI')
GROUP BY f.MeasurementDateKey
		  ,f.MeasurementTime
),

RainfallData AS
(
SELECT
 f.MeasurementDateKey
 ,f.MeasurementTime
 ,AVG(CONVERT(FLOAT,f.MeasurementValue)) AS Rainfall --mm
FROM NDS.Weather.factMetserviceWeatherMeasurement f
JOIN NDS.Weather.dimMetserviceElement e ON e.MetserviceElementKey = f.MetserviceElementKey
JOIN NDS.Weather.dimMetserviceLocation l ON l.MetserviceLocationKey = f.MetserviceLocationKey
WHERE
MeasurementElementType IN ('Observation')
AND e.MeasurementElementName IN ('Rainfall')
AND l.MeasurementLocationName IN ('AUCKLAND','HARBOUR BRIDGE','WHANGAPARAOA','WHENUAPAI')
GROUP BY f.MeasurementDateKey
		  ,f.MeasurementTime
),

SolarRadiationData AS
(
SELECT
 f.MeasurementDateKey
 ,f.MeasurementTime
 ,AVG(CONVERT(FLOAT,f.MeasurementValue)) AS SolarRadiation  --JCM2
FROM NDS.Weather.factMetserviceWeatherMeasurement f
JOIN NDS.Weather.dimMetserviceElement e ON e.MetserviceElementKey = f.MetserviceElementKey
JOIN NDS.Weather.dimMetserviceLocation l ON l.MetserviceLocationKey = f.MetserviceLocationKey
WHERE
MeasurementElementType IN ('Observation')
AND e.MeasurementElementName IN ('SolarRadiation')
AND l.MeasurementLocationName IN ('AUCKLAND','HARBOUR BRIDGE','WHANGAPARAOA','WHENUAPAI')
GROUP BY f.MeasurementDateKey
		  ,f.MeasurementTime
),

WindDirectionData AS
(
SELECT
 f.MeasurementDateKey
 ,f.MeasurementTime
 ,AVG(CONVERT(FLOAT,f.MeasurementValue)) AS WindDirection  --degrees
FROM NDS.Weather.factMetserviceWeatherMeasurement f
JOIN NDS.Weather.dimMetserviceElement e ON e.MetserviceElementKey = f.MetserviceElementKey
JOIN NDS.Weather.dimMetserviceLocation l ON l.MetserviceLocationKey = f.MetserviceLocationKey
WHERE
MeasurementElementType IN ('Observation')
AND e.MeasurementElementName IN ('WindDirection')
AND l.MeasurementLocationName IN ('AUCKLAND','HARBOUR BRIDGE','WHANGAPARAOA','WHENUAPAI')
GROUP BY f.MeasurementDateKey
		  ,f.MeasurementTime
),

WindGustData AS
(
SELECT
 f.MeasurementDateKey
 ,f.MeasurementTime
--,AVG(CONVERT(FLOAT,f.MeasurementValue)) AS WindGust  --knots
 ,AVG(CONVERT(FLOAT,f.MeasurementValue)*0.514444) AS WindGust --m/s 
--,AVG(CONVERT(FLOAT,f.MeasurementValue)*1.852) AS WindGust --km/h 
FROM NDS.Weather.factMetserviceWeatherMeasurement f
JOIN NDS.Weather.dimMetserviceElement e ON e.MetserviceElementKey = f.MetserviceElementKey
JOIN NDS.Weather.dimMetserviceLocation l ON l.MetserviceLocationKey = f.MetserviceLocationKey
WHERE
MeasurementElementType IN ('Observation')
AND e.MeasurementElementName IN ('WindGust') 
AND l.MeasurementLocationName IN ('AUCKLAND','HARBOUR BRIDGE','WHANGAPARAOA','WHENUAPAI')
GROUP BY f.MeasurementDateKey
		  ,f.MeasurementTime
),

WindSpeedData AS
(
SELECT
 f.MeasurementDateKey
 ,f.MeasurementTime
-- ,AVG(CONVERT(FLOAT,f.MeasurementValue)) AS WindSpeed  --knots
 ,AVG(CONVERT(FLOAT,f.MeasurementValue)*0.514444) AS WindSpeed  --m/s
-- ,AVG(CONVERT(FLOAT,f.MeasurementValue)*1.852) AS WindSpeed  --km/h
FROM NDS.Weather.factMetserviceWeatherMeasurement f
JOIN NDS.Weather.dimMetserviceElement e ON e.MetserviceElementKey = f.MetserviceElementKey
JOIN NDS.Weather.dimMetserviceLocation l ON l.MetserviceLocationKey = f.MetserviceLocationKey
WHERE
MeasurementElementType IN ('Observation')
AND e.MeasurementElementName IN ('WindSpeed')
AND l.MeasurementLocationName IN ('AUCKLAND','HARBOUR BRIDGE','WHANGAPARAOA','WHENUAPAI')
GROUP BY f.MeasurementDateKey
		  ,f.MeasurementTime
),

HourlyData AS
(
SELECT
td.DateKey
,td.TimeKey
,te.Temperature
,CASE WHEN te.Temperature < 18.0 THEN 18.0-te.Temperature ELSE 0 END AS HDD
,CASE WHEN te.Temperature > 18.0 THEN te.Temperature-18.0 ELSE 0 END AS CDD
,dp.Dewpoint
,ra.Rainfall
,sr.SolarRadiation
,wd.WindDirection
,wg.WindGust
,ws.WindSpeed
,100*(EXP((17.625*dp.Dewpoint)/(243.04+dp.Dewpoint))/EXP((17.625*te.Temperature)/(243.04+te.Temperature))) AS RelativeHumidity
--rho = RH*6.105*EXP(17.27*te.Temperature/(237.7+te.Temperature)) -- Water vapour pressure hPa
-- Version of the Steadman Apparent Temperature that does not not account for solar radiance
,te.Temperature+0.33*((EXP((17.625*dp.Dewpoint)/(243.04+dp.Dewpoint))/EXP((17.625*te.Temperature)/(243.04+te.Temperature)))*6.105*EXP(17.27*te.Temperature/(237.7+te.Temperature)))-0.70*ws.WindSpeed-4.00 AS ApparentTemperature
FROM TimeDimension td 
	LEFT JOIN TemperatureData te ON td.DateKey = te.MeasurementDateKey AND td.TimeKey = te.MeasurementTime
	LEFT JOIN DewpointData dp ON td.DateKey = dp.MeasurementDateKey AND td.TimeKey = dp.MeasurementTime 
	LEFT JOIN RainfallData ra ON td.DateKey = ra.MeasurementDateKey AND td.TimeKey = ra.MeasurementTime
	LEFT JOIN SolarRadiationData sr ON td.DateKey = sr.MeasurementDateKey AND td.TimeKey = sr.MeasurementTime
	LEFT JOIN WindDirectionData wd ON td.DateKey = wd.MeasurementDateKey AND td.TimeKey = wd.MeasurementTime
	LEFT JOIN WindGustData wg ON td.DateKey = wg.MeasurementDateKey AND td.TimeKey = wg.MeasurementTime
	LEFT JOIN WindSpeedData ws ON td.DateKey = ws.MeasurementDateKey AND td.TimeKey = ws.MeasurementTime
)

SELECT 
dd.DateKey
,dd.Year
,dd.Month
,dd.DayofMonth
,hd.TimeKey
,AVG(hd.Temperature) AS Tavg
,AVG(hd.ApparentTemperature) AS ApparentTemperature_avg
FROM NDS.VectorEM.dimDate dd
	JOIN HourlyData hd ON hd.DateKey = dd.DateKey
WHERE
--AND dd.DateKey = 20190612
dd.Year IN (2014, 2015,2016,2017,2018, 2019)
--AND td.TimeKey IN ('07:00:00.0000000')
-- AND dd.IsWeekend + CONVERT(INT,dd.IsNZNationalHoliday) + CONVERT(INT,dd.IsAucklandHoliday) = 0
--AND hd.TimeKey IN (
--'07:00:00.0000000'
--,'08:00:00.0000000'
--,'09:00:00.0000000'
--,'10:00:00.0000000'
--,'17:00:00.0000000'
--,'18:00:00.0000000'
--,'19:00:00.0000000'
--,'20:00:00.0000000'
--)
GROUP BY dd.DateKey
		  ,dd.Year
		  ,dd.Month
		  ,dd.DayofMonth
		  ,hd.TimeKey
ORDER BY 1,hd.TimeKey






