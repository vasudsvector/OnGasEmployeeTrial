# LPG-Bottle-Prediction
The codebase consists of code for both training the model and also for inferring from the model created. 

Main file for Train is train_main.py

Main file for Infer is main.py

## The Data folders Structure 
Input, Debug, State and SQL.

### Input:
Contains data of temperature with time, it needs to be pulled before running the code using Temperature.sql under SQL folder

Order data is pulled using factcylindermovement_3.sql under SQL folder

### Debug:
Contains file to debug a run.

cons.csv - File with predicted consumption appended for everyday

Error.csv - File containing the percentage error for each customer-order

### State
notify.json - This file contains the information about which customers need to be notified after the run is complete.

It stores last_run_date to make sure it restarts from the day to avoid running from scratch everytime.

It also stores which customers have been notified - this is to make sure that the customers notified already and not placed their order will not be notified over and over.
They will be removed from this list once they place their next order

It has list of customers to be notified/has been notified by date. At the moment, one has to manually look at the customers from the notification dates of interest.

### SubFolders
All the folders under data folder has four subfolders. 
1) 1_simp_avg:
The folder is for temperature bins at 1deg C intervals. simp_avg corresponds to the averaging technique used for generating coefficients for customers with less than 6 orders including new customers. The coefficient for these customers were derived by simply averaging from nearest neighbors based on survey data
2) 1_smoothed:
The folder is for temperature bins at 1deg C intervals. smoothed corresponds to the averaging technique used for generating coefficients for customers with less than 6 order including new customers. The coefficient for these customers were derived after removing customers with unusually large coefficient for a bin. These were derived manually.
3) 2_simp_avg:
The folder is for temperature bins at 2 deg C intervals. simp_avg explanation is as above
4) 2_smoothed  
The folder is for temperature bins at 2 deg C intervals. smoothed explanation is as above

The coefficients for each of these customers by temperature and technique is derived separately using Train module.