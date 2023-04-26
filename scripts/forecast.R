dt <- forecast_get_performance_data()

#sum(dt$dt_performance[OrderBrand == "Soda PDF" & Interval >= 200 & Interval < 451  & Cycle == 1 & Date >= '2021-05-01' & Date < '2021-06-01']$Scheduled)
#sum(dt$dt_backlog[OrderBrand == "Soda PDF" & Interval >= 200 & Interval < 451  & Cycle == 1 & RenewalMonth == '2021-05']$RenewalsCnt)

dt_past <- copy(dt$dt_performance)
dt_past <- process_past_rebills(dt_past = dt_past)

#sum(dt_past[Brand == "Soda PDF" & Interval >= 200 & Interval < 451  & Cycle == 1 & Renewal_year_month == '2021-05']$Scheduled)

dt_new_order <- forecast_get_new_sales_scheduled(dt_mapping = unique(unique(dt$dt_backlog[, c("OrderBrand", "Division"), with = FALSE], by = "OrderBrand")), last_date = dt$last_date)

Key_temp <- c("OrderBrand", "Division", "UserID")

dt_ref <- forecast_reference_rebill_rate_calculation(dt_performance = dt$dt_performance, last_date = dt$last_date, Key_temp = Key_temp)$dt_ref
dt_backlog <- rbind(cbind(dt$dt_backlog, existing_subscription = 1),
                    cbind(dt_new_order$dt_backlog, existing_subscription = 0))

dt_backlog_org <- copy(dt_backlog)

#sum(dt_backlog[OrderBrand == "Soda PDF" & Interval >= 200 & Interval < 451  & Cycle == 1 & RenewalMonth == '2021-05']$RenewalsCnt)

dt_all_backlog <- forecast_scheduled_and_future_cycles(dt_backlog, dt_ref, Key_temp = Key_temp)

#sum(dt_all_backlog[Brand == "Soda PDF" & Interval == 365  & Cycle == 1 & Renewal_year_month == '2021-05']$Scheduled)

dt_all_backlog <- rbind(dt_all_backlog, cbind(dt_past, existing_subscription = 1))

dt_all_backlog[, Processing_date := gsub("-", "",Sys.Date())]

dt_all_backlog[, Type := 1]




dt_ref_UD <- enrich_UD_ref(dt_ref, dt_ref_new = dt_new_order$dt_ref_new, enrich_UD_ref)

dt_all_backlog_UD <- forecast_scheduled_and_future_cycles(dt_backlog_org, dt_ref, Key_temp = Key_temp, dt_ref_UD)
dt_all_backlog_UD <- rbind(dt_all_backlog_UD, cbind(dt_past, existing_subscription = 1))
dt_all_backlog_UD[, Processing_date := gsub("-", "",Sys.Date())]
dt_ref[, Type := 1]
dt_all_backlog_UD[, Type := 2]
dt_ref_UD[, Type := 2]



dt_all_subs <- rbind(dt_all_backlog, dt_all_backlog_UD)

setnames(dt_ref, c("Origianl_Cycle", "OrderBrand"), c("Cycle", "Brand"))
dt_ref[, Processing_date := gsub("-", "",Sys.Date())]

dt_all_subs[is.nan(RebilledUSD), RebilledUSD := 0]
dt_all_subs[is.nan(ScheduledUSD), ScheduledUSD := 0]

dt_all_subs <- dt_all_subs[!(Rebilled == 0 & RebilledUSD == 0 & Scheduled == 0  & ScheduledUSD == 0 )]

q_ms_sql(paste0("DELETE FROM [AIReport].[Traffic].[forecast_details_SPG] where Processing_date = '", gsub("-", "", Sys.Date()), "'"), T)
q_ms_sql(paste0("DELETE FROM [AIReport].[Traffic].[forecast_details_SPG_V2] where Processing_date = '", gsub("-", "", Sys.Date()), "'"), T)
q_ms_sql(paste0("DELETE FROM [AIReport].[Traffic].[forecast_reference_SPG] where Processing_date = '", gsub("-", "", Sys.Date()), "'"), T)


dt_temp <- get_sum_col(dt_all_subs,
                       c("Renewal_year_month", "Division", "UserID", "Brand", "Cycle", "Interval", "Processing_date", "Type"),
                       c("Rebilled", "RebilledUSD", "Scheduled", "ScheduledUSD"))
setcolorder(dt_temp, names(dt_all_subs)[names(dt_all_subs) != "existing_subscription"])
write_Acquisition_ms_sql("forecast_details_SPG", dt_temp, db = "AIReport", Schema = "Traffic", AI_report = T)
write_Acquisition_ms_sql("forecast_reference_SPG", dt_ref, db = "AIReport", Schema = "Traffic", AI_report = T)

max_stable_month <- q_ms_sql(paste0("select max(Renewal_year_month) as d FROM [AIReport].[Traffic].[forecast_details_SPG_V2] where Processing_date = 0"), T)$d
dt_temp2 <- dt_all_subs[Renewal_year_month > max_stable_month]
dt_temp2[Renewal_year_month < substr(as.character(as.Date(dt$last_date) - 5), 1, 7), Processing_date := 0]
dt_temp2 <- get_sum_col(dt_temp2,
                        c("Renewal_year_month", "Division", "UserID", "Brand", "Cycle", "Interval", "existing_subscription", "Processing_date", "Type"),
                        c("Rebilled", "RebilledUSD", "Scheduled", "ScheduledUSD"))
setcolorder(dt_temp2, names(dt_all_subs)[names(dt_all_subs) != "existing_subscription"])

write_Acquisition_ms_sql("forecast_details_SPG_V2", dt_temp2, db = "AIReport", Schema = "Traffic", AI_report = T)

res <- get_sum_col(dt_all_subs[Type == 1 & Brand == "Soda PDF" & Renewal_year_month %like% "2021"], c("Renewal_year_month"), c("Rebilled", "RebilledUSD", "Scheduled", "ScheduledUSD"))
setorder(res, Renewal_year_month )
res

q_ms_sql(paste0("SELECT convert(varchar(7), Date, 126)  as renewal_month, sum(Scheduled) as Scheduled, sum(ScheduledUSD) as ScheduledUSD, sum(Rebilled) as Rebilled, sum(RebilledUSD) as RebilledUSD FROM [Report].[DM].[SubscriptionsPerformance] where Date >= '2022-01-01' and Date < '2023-01-01' and OrderBrand = 'Soda PDF' group by convert(varchar(7), Date, 126) order by renewal_month"))
res <- get_sum_col(dt_all_subs[Type == 1 & Brand == "Soda PDF" & Renewal_year_month %like% "2022"], c("Renewal_year_month"), c("Scheduled", "ScheduledUSD", "Rebilled", "RebilledUSD"))
setorder(res, Renewal_year_month )
res



dd <- get_sum_col(dt_all_subs[Type == 1], c("Division", "Brand", "Renewal_year_month"), c("Rebilled", "RebilledUSD", "Scheduled", "ScheduledUSD"))
setorder(dd, Division,  Brand, Renewal_year_month)
dd[, rate := Rebilled/Scheduled]
dd[Division == "PDF" & Renewal_year_month >= '2021-05' & Renewal_year_month < "2022-07"]
write_generic_csv_file(dt_all_subs[Type == 1 , -c("Type", "UserID"), with = FALSE], "C:/Users/behzad.beheshti/Google Drive/lulu/all_Rebills2.csv")


