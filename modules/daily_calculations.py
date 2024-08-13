from sources import *
from datetime import datetime
import datetime
import general_functions


# joins facility relevance to monthly estiamtions creates workable for daily estimations based on selected fafcilites
def ingest_data(excluded_facilities, baseline_year):

    # if there is no partial data, return empty
    daily_estimations.dropna(how='all', inplace=True)
    if len(daily_estimations.index) <= 1:
        empty_df = pd.DataFrame()
        return [empty_df,empty_df]

    print("\n Calculating Daily Estimations...\n")

    daily_estimations_raw = daily_estimations.merge(
        excluded_facilities[["Facility Name", "exclude_from_rates"]],
        on="Facility Name",
        how="left",
    )

    daily_estimations_workable = general_functions.sanitize_data_general(
        daily_estimations_raw, baseline_year
    )
    daily_estimations_workable = get_coverage(daily_estimations_workable)

    daily_estimations_workable["estimation_start_date"] = (
        daily_estimations_workable.apply(get_estimation_start_date, axis="columns")
    )
    daily_estimations_workable["estimation_end_date"] = (
        daily_estimations_workable.apply(get_estimation_end_date, axis="columns")
    )
    daily_estimations_workable["estimation_for_period"] = (
        daily_estimations_workable.apply(get_input_estimations, axis="columns")
    )

    # print(daily_estimations_workable)

    partial_data_index, partial_data_values = tabulate_daily_estimations(
        daily_estimations_workable
    )

    # print(partial_data_index)
    # print(partial_data_values)

    # TODO: REMOVE THIS PART FOR QA OLY
    #dt = dtale.show(daily_estimations_workable)
    # opens in browser
    #dt.open_browser()
    # input("Press enter to exit")

    return (partial_data_index, partial_data_values)


# calcualtes coverage for each entry
def get_coverage(workable_source):

    workable_source["days_without_emissions"] = workable_source.apply(
        days_without_emissions, axis="columns"
    )
    workable_source["is_partial"] = workable_source.apply(is_partial, axis="columns")

    return workable_source


# finds days without emissions per row
def days_without_emissions(row):
    number_of_days = (
        row["Total days in a month"] - row["Number of Days in Emissions Period"]
    )
    return number_of_days


# tags data as partial if doenst cover whole month
def is_partial(row):
    if row["days_without_emissions"] > 0:
        return True
    else:
        return False


# TODO: IMPROVE DAILIY ESTIMATIONS
# make it so they can grab partial data from any range in the month, not just if emissions starts on the first day and end in the last
# gets the dates and number of days that need estimation
def get_estimation_start_date(row):

    if row["is_partial"] == True:


        # extracts string from date
        date = general_functions.try_parsing_date(str(row["Emission Start Date"]))
        start_date_day = date.day
        start_date_month = date.month
        start_date_year = date.year

        # adds to start day so the emissions begin the day after the first
        start_date_day += 1

        emission_start_date = (
            str(start_date_day)
            + "/"
            + str(start_date_month)
            + "/"
            + str(start_date_year)
        )

        emission_start_date = datetime.datetime.strptime(emission_start_date, '%d/%m/%Y').strftime('%Y-%m-%d')
        return emission_start_date

    # if not using row for estimations, skip
    else:
        return ""


# gets end date for estimations
# TODO?: MAKE IT SO THAT IT DPOESNT ALWAYS END ON LAST DAY OF MONTH
def get_estimation_end_date(row):

    if row["is_partial"] == True:

        # extracts string from date
        date = general_functions.try_parsing_date(str(row["Emission End Date"]))
        end_date_day = row["Total days in a month"]
        end_date_month = date.month
        end_date_year = date.year
        emission_end_date = (
            str(end_date_day) + "/" + str(end_date_month) + "/" + str(end_date_year)
        )

        emission_end_date = datetime.datetime.strptime(emission_end_date, '%d/%m/%Y').strftime('%Y-%m-%d')
        return emission_end_date

    # if not using row for estimations, skip
    else:
        return ""


# calcualted estimated inputs based on values from available days
def get_input_estimations(row):

    if row["is_partial"] == True:
        # gets start and end days for both actual and estimation range dates
        actual_start_day = int(
            general_functions.try_parsing_date(row["Emission Start Date"]).day
        )
        actual_end_day = int(
            general_functions.try_parsing_date(row["Emission End Date"]).day
        )

        estimation_start_day = int(
            general_functions.try_parsing_date(row["estimation_start_date"]).day
        )
        estimation_end_day = int(
            general_functions.try_parsing_date(row["estimation_end_date"]).day
        )

        # First, calculates the input/days of the actual data, then multiplies it by number of days to estimate
        estimation = (
            row["Input Quantity"] / (actual_end_day - actual_start_day + 1)
        ) * (estimation_end_day - estimation_start_day + 1)

        return estimation
    else:
        return ""


# tabulates daily estimations to be used in other modules
def tabulate_daily_estimations(workable_source):

    # this one will be used to check if partial data exists for the period
    partial_data_index = workable_source[
        ["Facility Name", "Month", "Financial Year", "estimation_for_period"]
    ].copy()
    partial_data_index = (
        partial_data_index.groupby(["Facility Name", "Financial Year", "Month"])[
            "estimation_for_period"
        ].sum()
    ).reset_index()
    partial_data_index = partial_data_index.drop(
        partial_data_index[partial_data_index["estimation_for_period"] == ""].index
    )

    # this df is where values and dates will actually get pulled from, mostly to include start and end dates
    partial_data_values = workable_source[
        [
            "Facility Name",
            "Month",
            "Financial Year",
            "estimation_start_date",
            "estimation_end_date",
            "estimation_for_period",
            "Input Unit",
        ]
    ].copy()
    partial_data_values = partial_data_values.drop(
        partial_data_values[partial_data_values["estimation_for_period"] == ""].index
    )
    return (partial_data_index, partial_data_values)
