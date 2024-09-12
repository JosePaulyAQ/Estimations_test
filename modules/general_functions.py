import numpy as np
import pandas as pd
from datetime import datetime
import calendar
from sources import *


# Sanitizes data for CAPACITY FTE use
def sanitize_data_general(raw_source, baseline_year):

    # ----------- FOR MONTHLY ------------------------#

    if "is_relevant" in raw_source.columns:
        # marks entries as valid if they have no emiissions and thier facility is relevant
        raw_source["use_in_estimations"] = raw_source.apply(is_valid, axis="columns")

        raw_source = raw_source.drop(
            raw_source[raw_source["use_in_estimations"] == False].index
        )

        # fill in consumption_rate type with data as dictated by FUTURE input template
        raw_source["consumption_rate_type"] = raw_source.apply(
            lambda r: add_missing_consumption_type(
                r, selected_FTE_facilities_and_activities
            ),
            axis="columns",
        )
    # -------------------------------------------------------#

    if "exclude_from_rates" in raw_source.columns:
        raw_source = raw_source.drop(
            raw_source[raw_source["exclude_from_rates"] == True].index
        )

    # removed unsed columns
    raw_source = raw_source.loc[:, ~raw_source.columns.str.contains("^Unnamed")]

    # removes commas, and turns to float so operations can be done

    column_list = ["FTE", "Capacity (m2)", "Input Quantity", "Emission Quantity"]

    year_list = ["Financial Year", "Year", "Emission Year"]

    for column in column_list:

        if column in raw_source.columns:
            raw_source[column] = (
                raw_source[column].replace(",", "", regex=True).astype(float)
            )
            if column == "FTE" or column == "Capacity (m2)":
                raw_source = raw_source.dropna(subset=["FTE", "Capacity (m2)"])
                raw_source["Capacity (m2)"] = raw_source["Capacity (m2)"].replace(
                    0, np.nan
                )
                raw_source["FTE"] = raw_source["FTE"].replace(0, np.nan)

    for year in year_list:
        if year in raw_source.columns:
            raw_source[year] = raw_source[year].replace(",", "", regex=True).astype(int)

            raw_source = raw_source.loc[raw_source[year] >= baseline_year]

    workable_source = raw_source.reset_index()
    return workable_source


# ----------SUPPORT FOR SANITIZATION-------------#
# checks if all rows have a usable consumption type value
def add_missing_consumption_type(row, FTE_selection):

    permited_types = ["FTE", "capacity"]

    # reads from customer FTE selection excel
    type_list = FTE_selection["facility_type"].unique()
    activity_list = FTE_selection["activity"].unique()

    if row["consumption_rate_type"] in permited_types:
        return row["consumption_rate_type"]
    else:
        if row["Type"] in type_list and row["Sub Activity"] in activity_list:
            return "FTE"
        else:
            return "capacity"


# aditional criteria for validity
def is_valid(row):

    # this marks entries as valid if they have no emission and thier facility is relevant
    valid = row["is_relevant"] and (row["Data Availability"] == ("No Data"))

    return valid


# ----------SUPPORT FOR DATES-------------------#
# checks for different date formats
from datetime import datetime

def try_parsing_date(text):

   # text = text.strip().title()  # Ensure the text is in title case and remove extra spaces
    if not text:
        print("Empty date string found")
        raise ValueError("Empty date string found")
    for fmt in (
        "%Y-%m-%d",
        "%y-%m-%d",
        "%d.%m.%Y",
        "%d.%m.%y",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%d-%b-%y",
        "%d-%b-%Y",
        "%d %B,%Y",
        "%d %B,%y",
        "%b %d, %Y",
        "%b %d, %y",
        "%B %d, %Y",
        "%B %d, %y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%y/%m/%d",
        "%d-%m-%y",
        "%m-%d-%y",
        "%d %B, %Y",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    print(f"Invalid date format found: {text}")
    raise ValueError(
        "no valid date format found: " + text + " Please check ..\modules\general_functions.try_parsing_date , and add your desired format"
    )