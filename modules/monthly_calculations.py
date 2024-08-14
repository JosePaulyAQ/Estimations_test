from sources import *
from datetime import datetime
import calendar
import general_functions


def ingest_data(
    selected_facilities, baseline_year, capacity_and_FTE_outputs, rates_outputs
):

    print("\n Calculating Monthly Estimations...\n")

    # use necesary df from other modules
    # rates_outputs = [rates_workable,BU_and_type_rates,type_rates,BU_rates,activity_rates]
    rates_general = rates_outputs[0]

    # TODO: FIX RELEVANCY CHOOSING
    monthly_estimations_raw = monthly_estimations.merge(
        selected_facilities[["Facility Name", "is_relevant"]],
        on="Facility Name",
        how="left",
    )

    second_merge = monthly_estimations_raw.merge(
        rates_general[["Facility Name", "consumption_rate_type"]],
        on="Facility Name",
        how="left",
    )

    monthly_estimations_workable = general_functions.sanitize_data_general(
        second_merge, baseline_year  # , FTE_selection
    )

    # sets new columns, from rates dfs
    # monthly_estimations_workable['unit']=rates_general.loc[0,'Input Unit']
    monthly_estimations_workable["unit"] = monthly_estimations_workable.apply(
        lambda r: get_unit(r, rates_outputs), axis="columns"
    )

    monthly_estimations_workable["rate"] = monthly_estimations_workable.apply(
        lambda r: get_rate(r, rates_outputs), axis="columns"
    )

    monthly_estimations_workable["estimation_for_period"] = (
        monthly_estimations_workable.apply(
            lambda r: get_estimation(r, capacity_and_FTE_outputs), axis="columns"
        )
    )
    monthly_estimations_workable["end_day"] = monthly_estimations_workable.apply(
        get_end_day, axis="columns"
    )

    monthly_estimations_workable["first_day"] = monthly_estimations_workable.apply(
        get_first_day, axis="columns"
    )

    # TODO: REMOVE; FOR QA ONLY
    monthly_estimations_workable["rate used"] = monthly_estimations_workable.apply(
        lambda r: get_rate_map_TEST(r, rates_outputs), axis="columns"
    )

    monthly_estimations_output = monthly_estimations_workable.copy()
    monthly_estimations_output = (
        monthly_estimations_output.groupby(
            ["Facility Name", "Sub Activity", "Emission Year", "Emission Month", "unit"]
        )["estimation_for_period"]
        .mean()
        .reset_index()
    )

    # this df is used to map facility name, year, and month, to facilities that close/end before emission period finishes
    # AKA: used to find entries wiht incomplete days
    end_day_mapping = monthly_estimations_workable.copy()
    end_day_mapping = end_day_mapping.groupby(
        ["Facility Name", "Emission Year", "Emission Month"]
    )["end_day"].mean()

    first_day_mapping = monthly_estimations_workable.copy()
    first_day_mapping = first_day_mapping.groupby(
        ["Facility Name", "Emission Year", "Emission Month"]
    )["first_day"].mean()


    # TODO: REMOVE THIS PART FOR QA OLY

    #dn = dtale.show(first_day_mapping)
    # opens in browser
    #dn.open_browser()
    print('\n Done Calculating! \n')
    input("\n Press enter to generate Output \n")

    return (monthly_estimations_output, end_day_mapping,first_day_mapping)


# gets the apropriate unit for each row
def get_unit(row, rates_outputs):

    row_BU_and_type = [row["BU Name"], row["Type"]]
    row_type = [row["Type"]]
    row_BU = [row["BU Name"]]
    row_year = row["Emission Year"]

    row_BU_and_type_index = (row["BU Name"], row["Type"], row["Emission Year"])
    row_type_index = (row["Type"], row["Emission Year"])
    row_BU_index = (row["BU Name"], row["Emission Year"])

    # data grouped together to be referenced in for loop
    list_of_indexes = [
        row_BU_and_type_index,
        row_type_index,
        row_BU_index,
    ]

    list_of_params = [row_BU_and_type, row_type, row_BU]

    list_of_units = [rates_outputs[7], rates_outputs[8], rates_outputs[9]]
    rates_workable = rates_outputs[0]
    activity_units = rates_outputs[10]

    # first, checks if its directly available from rates_workable
    if row["Facility Name"] in rates_workable["Facility Name"].unique():

        unit = rates_workable.loc[
            rates_workable["Facility Name"] == row["Facility Name"], "Input Unit"
        ].iloc[0]
        return unit

    for param, index, units in zip(list_of_params, list_of_indexes, list_of_units):

        param_previous_year = tuple(param + [row_year - 1])
        tupple_param = tuple(param)

        if tupple_param in units.index:

            # check if there is data for current year
            if index in units.index:
                return units.loc[index].idxmax()

            # check if there is data for previous year
            elif param_previous_year in units.index:
                return units.loc[param_previous_year].idxmax()

            # if none of the above, use average across years
            else:
                # here, it is accessed as a tupple since the idxmax being claculated is
                # for all years
                # so grab just second element of tupple, whic his actual unit
                unit = units[tupple_param].idxmax()
                return unit[1]

    # if nothing is found, use activity wide average
    unit = activity_units.loc[row["Sub Activity"]].idxmax()
    return unit[1]



# gets tfinal day oof emissions based on month and year matching the closeing date
def get_end_day(row):

    row_year = row["Emission Year"]
    row_month = row["Emission Month"]
    

    date = general_functions.try_parsing_date(row["Closed/End of Emissions Date"])


    end_date_day = date.day
    end_date_month = date.month
    end_date_year = date.year

    # holds how many days should be in a given month
    days_in_month = calendar.monthrange(row_year, row_month)[1]

    # checks if there are as many dyas in reporting period as in the month
    if end_date_month==row_month and end_date_year==row_year and end_date_day < days_in_month:
        return end_date_day
    else:
        return days_in_month



# gets the first day that the facility was open
def get_first_day(row):

    row_year = row["Emission Year"]
    row_month = row["Emission Month"]
    

    date = general_functions.try_parsing_date(row["Opening Date"])


    end_date_day = date.day
    end_date_month = date.month
    end_date_year = date.year


    # checks if there are as many dyas in reporting period as in the month
    if end_date_month==row_month and end_date_year==row_year and end_date_day != 1:
        return end_date_day
    else:
        return 1


# TODO: TEST FUNCTION, REMOVE
def get_rate_map_TEST(row, rates_outputs):

    # use necesary df from other modules
    facility_rates_df = rates_outputs[1]
    BU_and_type_df = rates_outputs[2]
    type_df = rates_outputs[3]
    BU_df = rates_outputs[4]
    activity_df = rates_outputs[5]

    row_BU = row["BU Name"]
    row_type = row["Type"]
    row_year = row["Emission Year"]
    row_name = row["Facility Name"]

    # this value is used to check if this specific combination of facility, year, and month exists in the FTE  and capacity dfs
    row_index_combined = (row_BU, row_type)

    if row_name in facility_rates_df.index and (facility_rates_df[row_name].mean()) > 0:

        # check if there is data for current year
        if (row_name, row_year) in facility_rates_df.index and facility_rates_df.loc[
            (row_name, row_year)
        ].mean() > 0:

            return str(row_name) + "->" + str(row_year)

        # check if there is data for previous year
        elif (
            row_name,
            (row_year - 1),
        ) in facility_rates_df.index and facility_rates_df.loc[
            (row_name, (row_year - 1))
        ].mean() > 0:
            return str(row_name) + "->" + str(row_year - 1)

        # if none of the above, use average across years
        elif row_name in facility_rates_df.index:
            return str(row_name) + "-> years average"

    # check match for BU and type, and checks if average would be 0
    elif (
        row_index_combined in BU_and_type_df.index
        and (BU_and_type_df[row_index_combined].mean()) > 0
    ):

        # check if there is data for current year
        if (row_BU, row_type, row_year) in BU_and_type_df.index and BU_and_type_df.loc[
            (row_BU, row_type, row_year)
        ].mean() > 0:
            return str(row_BU) + "->" + str(row_type) + "->" + str(row_year)

        # check if there is data for previous year
        elif (
            row_BU,
            row_type,
            (row_year - 1),
        ) in BU_and_type_df.index and BU_and_type_df.loc[
            (row_BU, row_type, (row_year - 1))
        ].mean() > 0:
            return str(row_BU) + "->" + str(row_type) + "->" + str(row_year - 1)

        # if none of the above, use average across years
        elif row_index_combined in BU_and_type_df.index:
            return str(row_BU) + "->" + str(row_type) + "-> all years average"

    # do the same checks on type DF
    elif row_type in type_df.index and type_df[row_type].mean() > 0:

        if (row_type, row_year) in type_df.index and type_df.loc[
            (row_type, row_year)
        ].mean() > 0:
            return str(row_type) + "->" + str(row_year)

        elif (row_type, (row_year - 1)) in type_df.index and type_df.loc[
            (row_type, (row_year - 1))
        ].mean() > 0:
            return str(row_type) + "->" + str(row_year - 1)
        else:
            return str(row_type) + "-> all years average"

    # do the same checks on BU df
    elif row_BU in BU_df.index:

        if (row_BU, row_year) in BU_df.index and BU_df.loc[
            (row_BU, row_year)
        ].mean() > 0:
            return str(row_BU) + "->" + str(row_year)

        elif (row_BU, (row_year - 1)) in BU_df.index and BU_df.loc[
            (row_BU, (row_year - 1))
        ].mean() > 0:
            return str(row_BU) + "->" + str(row_year - 1)

        else:
            return str(row_BU) + "-> all years average"

    # return activity averages
    else:
        if row_year in activity_df.index and activity_df[row_year].mean() > 0:

            return str(row["Sub Activity"]) + "->" + str(row_year)

        if (row_year - 1) in activity_df.index and activity_df[
            (row_year - 1)
        ].mean() > 0:
            return str(row["Sub Activity"]) + "->" + str(row_year - 1)

        else:
            return str(row["Sub Activity"]) + "-> all years average"


# gets the apropriate rate, based on original hirearachy
# this is where the actual hierachy is applied,
def get_rate(row, rates_outputs):

    row_facility = [row["Facility Name"]]
    row_BU_and_type = [row["BU Name"], row["Type"]]
    row_type = [row["Type"]]
    row_BU = [row["BU Name"]]
    row_activity = [row["Sub Activity"]]

    row_year = row["Emission Year"]

    # data grouped together to be referenced in for loop
    list_of_params = [row_facility, row_BU_and_type, row_type, row_BU, row_activity]
    # [facility_rates, BU_and_type_rates, type_rates,BU_Rates, activity_rates]
    list_of_rates = [
        rates_outputs[1],
        rates_outputs[2],
        rates_outputs[3],
        rates_outputs[4],
        rates_outputs[5],
    ]

    for param, rate in zip(list_of_params, list_of_rates):

        param_with_year = tuple(param + [row_year])
        param_previous_year = tuple(param + [row_year - 1])
        # used to combine [BU, type] var into tupple to be used as index
        tupple_param = tuple(param)

        # checks if current parametre is in the corresponding rate table
        if tupple_param in rate.index and (rate[tupple_param].mean()) > 0:

            # check if there is data for current year
            if param_with_year in rate.index and rate.loc[param_with_year].mean() > 0:
                return rate.loc[param_with_year].mean()

            # check if there is data for previous year
            elif (
                param_previous_year in rate.index
                and rate.loc[param_previous_year].mean() > 0
            ):
                return rate.loc[param_previous_year].mean()

            # if none of the above, use average across years
            else:
                return rate[tupple_param].mean()


def get_operation_percentage(row_close_date, row_year, row_month):

    date = general_functions.try_parsing_date(row_close_date)
    end_date_day = date.day
    end_date_month = date.month
    end_date_year = date.year

    # holds how many days should be in a given month
    days_in_month = calendar.monthrange(row_year, row_month)[1]

    if end_date_year == row_year and end_date_month == row_month:
        # checks if there are as many days in reporting period as in the month
        if end_date_day < days_in_month:

            return end_date_day / days_in_month
        else:
            return 1
    else:
        return 1


# multiplies the previously chosen rate by the most relevant FTE or capacity found
def get_estimation(row, capacity_and_FTE_outputs):

    # [FTE_name_avg, FTE_BU_type_avg, FTE_type_avg, FTE_BU_avg]
    FTE_averages = [
        capacity_and_FTE_outputs[1],
        capacity_and_FTE_outputs[3],
        capacity_and_FTE_outputs[5],
        capacity_and_FTE_outputs[7],
    ]

    # [capacity_name_avg, capacity_BU_type_avg, capacity_type_avg, capacity_BU_avg]
    capacity_averages = [
        capacity_and_FTE_outputs[0],
        capacity_and_FTE_outputs[2],
        capacity_and_FTE_outputs[4],
        capacity_and_FTE_outputs[6],
    ]

    row_year = row["Emission Year"]
    row_month = row["Emission Month"]
    row_rate = row["rate"]

    row_facility = [row["Facility Name"]]
    row_BU_and_type = [row["BU Name"], row["Type"]]
    row_type = [row["Type"]]
    row_BU = [row["BU Name"]]

    # determines % of days of the month that the facility was open
    monthly_operation_percentage = get_operation_percentage(
        row["Closed/End of Emissions Date"], row_year, row_month
    )

    # indexing used in for loops, ORDER IS IMPORTANT
    # the index of each element in list_of_indexes must match list_of Parametres
    # EG: row_name_index should be [0] in list_of_indexes
    # and row_facility [0] in list_of_params
    row_name_index = (row["Facility Name"], row["Emission Year"], row["Emission Month"])

    row_BU_and_type_index = (
        row["BU Name"],
        row["Type"],
        row["Emission Year"],
        row["Emission Month"],
    )
    row_type_index = (row["Type"], row["Emission Year"], row["Emission Month"])

    row_BU_index = (row["BU Name"], row["Emission Year"], row["Emission Month"])

    list_of_indexes = [
        row_name_index,
        row_BU_and_type_index,
        row_type_index,
        row_BU_index,
    ]

    list_of_params = [row_facility, row_BU_and_type, row_type, row_BU]

    # determines wether to use FTE or capacity avgs tables
    if row["consumption_rate_type"] == "FTE":
        FTE_or_capacity_avgs = FTE_averages
    else:
        FTE_or_capacity_avgs = capacity_averages

    for param, index, avg_df in zip(
        list_of_params, list_of_indexes, FTE_or_capacity_avgs
    ):
        param_with_year = tuple(param + [row_year])
        param_previous_year = tuple(param + [row_year - 1])
        tupple_param = tuple(param)

        # checks if current parametre has a match in its corresponding FTE or Rates table
        if tupple_param in avg_df.index:

            # if yes, check if there is month matching data
            if index in avg_df.index and avg_df[index] > 0:

                selected_average = avg_df[index]
                return selected_average * row_rate * monthly_operation_percentage

            # if not, checks if matching year
            elif (
                param_with_year in avg_df.index
                and avg_df.loc[param_with_year].mean() > 0
            ):

                selected_average = avg_df.loc[param_with_year].mean()
                return selected_average * row_rate * monthly_operation_percentage

            # if not, checks if previous year
            elif (
                param_previous_year in avg_df.index
                and avg_df.loc[param_previous_year].mean() > 0
            ):

                selected_average = avg_df.loc[param_previous_year].mean()
                return selected_average * row_rate * monthly_operation_percentage

            # if not, uses all years average
            else:
                selected_average = avg_df.loc[tupple_param].mean()
                return selected_average * row_rate * monthly_operation_percentage
