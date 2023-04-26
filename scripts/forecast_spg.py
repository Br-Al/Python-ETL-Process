import pandas as pd

#Get performance data
dt = forecast_get_performance_data()

#Sum of scheduled values with given conditions
sum_scheduled = (dt.query('OrderBrand == "Soda PDF" & Interval >= 200 & Interval < 451  & Cycle == 1 & Date >= "2021-05-01" & Date < "2021-06-01"')['Scheduled']).sum()
print(sum_scheduled)

#Sum of Renewals count with given conditions
sum_renewals = (dt.query('OrderBrand == "Soda PDF" & Interval >= 200 & Interval < 451  & Cycle == 1 & RenewalMonth == "2021-05"')['RenewalsCnt']).sum()
print(sum_renewals)

#Copy the dt_performance dataframe first
dt_past = dt.copy()

#Process past rebills
dt_past = process_past_rebills(dt_past=dt_past)

#Sum of Scheduled values with given conditions
sum_scheduled_new = (dt_past.query('Brand == "Soda PDF" & Interval >= 200 & Interval < 451  & Cycle == 1 & Renewal_year_month == "2021-05"')['Scheduled']).sum()
print(sum_scheduled_new)

#Get new sales scheduled
dt_new_order = forecast_get_new_sales_scheduled( dt_mapping =pd.unique(pd.unique(dt['OrderBrand','Division'], with = False) , by='OrderBrand'), last_date = dt['last_date'])

#tempoary Key columns
Key_temp = ["OrderBrand", "Division", "UserID"]

# Calculate reference rebill rate
dt_ref = forecast_reference_rebill_rate_calculation(dt_performance = dt['dt_performance'], last_date = dt['last_date'], Key_temp = Key_temp)['dt_ref']

#Creating a combine dataframe with existing subscriptions and new ones
dt_backlog = pd.concat([pd.concat([dt['dt_backlog'],existing_subscription=1]), pd.concat([dt_new_order['dt_backlog'], existing_subscription=0])])

#Back up dataframe for reclaiming future cycles
dt_backlog_org = dt_backlog.copy()

#Sum of Renewals count with given conditions
sum_renewals_2 = (dt_backlog.query('OrderBrand == "Soda PDF" & Interval >= 200 & Interval < 451  & Cycle == 1 & RenewalMonth == "2021-05"')['RenewalsCnt']).sum()
print(sum_renewals_2)

#Calculate Scheduled as well as future cycle values
dt_all_backlog = forecast_scheduled_and_future_cycles(dt_backlog, dt_ref, Key_temp =  Key_temp)

#Sum of Scheduled values in Future cycles with given conditions
sum_scheduled_future = (dt_all_backlog.query('Brand == "Soda PDF" & Interval == 365  & Cycle == 1 & Renewal_year_month == "2021-05"')['Scheduled']).sum()
print(sum_scheduled_future)


#Adding past subscription to dt_all_backlog data frame 
dt_all_backlog = pd.concat([dt_all_backlog,pd.concat([dt_past,existing_subscription=1])])

#Adding Rows Processing date and Type
dt_all_backlog['Processing_date'] = datetime.today().strftime('%Y%m%d')
dt_all_backlog['Type'] = 1
    
#Delete all rows from certains tables with certain condition
q_ms_sql('DELETE FROM [AIReport].[Traffic].[forecast_details_SPG] where Processing_date = '+ datetime.today().str 
