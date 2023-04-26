import pandas as pd


def enrich_ud_ref(data_ref_new, data_org_backlog):
    data_org = data_org_backlog[["Renewal_year_month", "Brand", "Scheduled", "Rebilled"]].groupby(["Renewal_year_month", "Brand"]).sum().reset_index()
    data_org.loc[:, "rate"] = data_org["Rebilled"] / data_org["Scheduled"]

    data_ref_new.loc[:, "Brand"] = data_ref_new["Title"].str.replace("%Renewal ", "").str.replace("Monthly ", "").str.replace("Yearly ", "")
    data_ref_new.loc[:, "Renewal_year_month"] = "20" + data_ref_new["year"] + "-" + data_ref_new["Month"].apply(lambda x: str(x).zfill(2))
    data_ref_new = data_ref_new[["Renewal_year_month", "Value", "Brand"]]

    data_ref_temp = pd.merge(data_org, data_ref_new[["Brand", "Renewal_year_month", "Value"]].drop_duplicates(), on=["Brand", "Renewal_year_month"], how="outer")
    
    data_ref_temp.loc[:, "Deviation"] = (data_ref_temp["Value"] - data_ref_temp["rate"]) / data_ref_temp["Value"]
    data_ref_temp = data_ref_temp.loc[data_ref_temp["Deviation"] != 0, ["OrderBrand", "Renewal_year_month", "Deviation"]]
    data_ref_temp.rename(columns={"Brand": "OrderBrand"}, inplace=True)

    return data_ref_temp
