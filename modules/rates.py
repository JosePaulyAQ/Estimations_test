from sources import *
import general_functions

def ingest_data(excluded_facilities, baseline_year, capacity_and_FTE_outputs,custom_rate_selected):

    print("\n Calculating Rates...\n")

    rates_raw = rates_and_facilities.merge(
        excluded_facilities[["Facility Name", "exclude_from_rates"]],
        on="Facility Name",
        how="left",
    )
    rates_workable = general_functions.sanitize_data_general(rates_raw, baseline_year)

    # Brings in the FTE selection from excel template
    FTE_selection = selected_FTE_facilities_and_activities

    # imports rate value and rate type from rates_workable
    rates_workable["consumption_rate_type"] = rates_workable.apply(
        lambda r: get_rate_type(r, FTE_selection),
        axis="columns",
    )
    rates_workable["consumption_rate"] = rates_workable.apply(
        lambda r: get_rate(r, capacity_and_FTE_outputs, FTE_selection,custom_rate_selected), axis="columns"
    )

    #TODO: DELETE, TEST FUNCT
    rates_workable["CAPACITY_USED"] = rates_workable.apply(
        lambda r: get_CAPACITY_TEST(r, capacity_and_FTE_outputs, FTE_selection,custom_rate_selected), axis="columns"
    )
    #TODO: DELETE, TEST FUNCT
    rates_workable["CAPACITY_NAME"] = rates_workable.apply(
        lambda r: get_CAPACITY_NAME_TEST(r, capacity_and_FTE_outputs, FTE_selection,custom_rate_selected), axis="columns"
    )


    # creates dfs for future reference
    facility_rates, BU_and_type_rates, type_rates, BU_rates, activity_rates = (
        tabulate_rates(rates_workable)
    )

    # creates dfs for most common unit types
    facility_units, BU_and_type_units, type_units, BU_units, activity_units = (
        tabulate_units(rates_workable)
    )

    # print(rates_workable)
    #print(BU_and_type_rates)
    #print(BU_rates)
    #print(type_rates)
    #print(activity_rates)
    #print(facility_rates)

    # TODO: REMOVE THIS PART FOR QA OLY
    
    dt = dtale.show(rates_workable)
    # opens in browser
    dt.open_browser()
    #input("\n Press enter to continue...\n")

    return (
        rates_workable,
        facility_rates,
        BU_and_type_rates,
        type_rates,
        BU_rates,
        activity_rates,
        facility_units,
        BU_and_type_units,
        type_units,
        BU_units,
        activity_units,
    )


# creates a df for BU, type, BU+type, and ativity level rates, per year
def tabulate_rates(workable_df):
    # creates new table, arranges indexes and calculates averages
    # the _exp suffix is used to diferentiate from global variables being exported, for easy of user comprhension
    facility_rates_exp = workable_df[
        ["Facility Name", "Emission Year", "consumption_rate", "Input Unit"]
    ].copy()
    facility_rates_exp = facility_rates_exp.groupby(
        ["Facility Name", "Emission Year", "Input Unit"]
    )["consumption_rate"].mean()

    BU_and_type_rates_exp = workable_df[
        ["BU Name", "Type", "Emission Year", "consumption_rate", "Input Unit"]
    ].copy()
    BU_and_type_rates_exp = BU_and_type_rates_exp.groupby(
        ["BU Name", "Type", "Emission Year", "Input Unit"]
    )["consumption_rate"].mean()

    BU_rates_exp = workable_df[
        ["BU Name", "Emission Year", "consumption_rate", "Input Unit"]
    ].copy()
    BU_rates_exp = BU_rates_exp.groupby(["BU Name", "Emission Year", "Input Unit"])[
        "consumption_rate"
    ].mean()

    type_rates_exp = workable_df[
        ["Type", "Emission Year", "consumption_rate", "Input Unit"]
    ].copy()
    type_rates_exp = type_rates_exp.groupby(["Type", "Emission Year", "Input Unit"])[
        "consumption_rate"
    ].mean()

    activity_rates_exp = workable_df[
        ["Sub Activity", "Emission Year", "consumption_rate", "Input Unit"]
    ].copy()
    activity_rates_exp = activity_rates_exp.groupby(
        ["Sub Activity", "Emission Year", "Input Unit"]
    )["consumption_rate"].mean()

    return (
        facility_rates_exp,
        BU_and_type_rates_exp,
        type_rates_exp,
        BU_rates_exp,
        activity_rates_exp,
    )


# classifies the units in similar ways to the rates
def tabulate_units(workable_df):

    # creates new table, arranges indexes and calculates rates
    # the _exp suffix is used to diferentiate from global variables being exported, for easy of user comprhension
    facility_units_exp = workable_df[
        ["Facility Name", "Emission Year", "consumption_rate", "Input Unit"]
    ].copy()
    facility_units_exp = facility_units_exp.groupby(
        ["Facility Name", "Emission Year", "Input Unit"]
    )["Input Unit"].count()

    BU_and_type_units_exp = workable_df[
        ["BU Name", "Type", "Emission Year", "consumption_rate", "Input Unit"]
    ].copy()
    BU_and_type_units_exp = BU_and_type_units_exp.groupby(
        ["BU Name", "Type", "Emission Year", "Input Unit"]
    )["Input Unit"].count()

    BU_units_exp = workable_df[
        ["BU Name", "Emission Year", "consumption_rate", "Input Unit"]
    ].copy()
    BU_units_exp = BU_units_exp.groupby(["BU Name", "Emission Year", "Input Unit"])[
        "Input Unit"
    ].count()

    type_units_exp = workable_df[
        ["Type", "Emission Year", "consumption_rate", "Input Unit"]
    ].copy()
    type_units_exp = type_units_exp.groupby(["Type", "Emission Year", "Input Unit"])[
        "Input Unit"
    ].count()

    activity_units_exp = workable_df[
        ["Sub Activity", "Emission Year", "consumption_rate", "Input Unit"]
    ].copy()
    activity_units_exp = activity_units_exp.groupby(
        ["Sub Activity", "Emission Year", "Input Unit"]
    )["Input Unit"].count()

    return (
        facility_units_exp,
        BU_and_type_units_exp,
        type_units_exp,
        BU_units_exp,
        activity_units_exp,
    )


# calcualtes rates for each row, following hiereachy order for avialable FTE or capacity
def get_rate(row, capacity_and_FTE_outputs, FTE_selection,custom_rate_selected):

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

    row_input = row["Input Quantity"]
    row_year = row["Emission Year"]

    row_facility = [row["Facility Name"]]
    row_BU_and_type = [row["BU Name"], row["Type"]]
    row_type = [row["Type"]]
    row_BU = [row["BU Name"]]

    row_name_index = (row["Facility Name"], row["Emission Year"], row["Emission Month"])

    row_BU_and_type_index = (
        row["BU Name"],
        row["Type"],
        row["Emission Year"],
        row["Emission Month"],
    )
    row_type_index = (row["Type"], row["Emission Year"], row["Emission Month"])

    row_BU_index = (row["BU Name"], row["Emission Year"], row["Emission Month"])

    # data grouped together to be referenced in for loop
    list_of_indexes = [
        row_name_index,
        row_BU_and_type_index,
        row_type_index,
        row_BU_index,
    ]

    list_of_params = [row_facility, row_BU_and_type, row_type, row_BU]

    # reads from customer FTE selection excel

    type_list = FTE_selection["facility_type"].unique()
    activity_list = FTE_selection["activity"].unique()

    # hierarchy for choosing FTE and Capacity is the same as for choosing a rate, as show in documentation
    # first, checks if facility should use FTE or capacity, and gets apropriate global average
    if row["Sub Activity"] in activity_list and row_type in type_list:
        FTE_or_capacity_avgs = FTE_averages

        unindex=FTE_averages[0].reset_index()

        global_average=unindex.loc[:,'FTE'].mean()

    else:
        FTE_or_capacity_avgs = capacity_averages

        unindex=capacity_averages[0].reset_index()

        global_average=unindex.loc[:,'Capacity (m2)'].mean()
        

    #check if custom rate was selected, if not, uses normal hieararchy, if yes, replaces all rates with Cutom one
    #TODO: Make it so user can choose which faiclities to use custom rate for
    if custom_rate_selected==False:
        # runs through each parametre, in hierarchy order, to find mos suitable FTE or capacity
        for param, index, avg_df in zip(
            list_of_params, list_of_indexes, FTE_or_capacity_avgs
        ):

            param_with_year = tuple(param + [row_year])
            tupple_param = tuple(param)

            # checks if current parametre has a match in its corresponding FTE or Rates table
            if tupple_param in avg_df.index:

                # row month and year found in FTE df?
                if index in avg_df.index and avg_df[index] > 0:
                    return row_input / avg_df[index]

                # if no month match, check if there is yearly avg

                elif (
                    param_with_year in avg_df.index
                    and avg_df.loc[param_with_year].mean() > 0
                ):
                    return row_input / (avg_df.loc[param_with_year].mean())

                # if no year match, use all yrs avg
                elif (avg_df.loc[tupple_param].mean()>0):
                    return row_input / (avg_df.loc[tupple_param].mean())
                
        #if all else fails, returns a global average
        return (row_input / global_average)    
    else:
        return(FTE_selection["custom_rate"][0])

#TODO: TEST FUNCT,DELETE

# calcualtes rates for each row, following hiereachy order for avialable FTE or capacity
def get_CAPACITY_TEST(row, capacity_and_FTE_outputs, FTE_selection,custom_rate_selected):

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

    row_input = row["Input Quantity"]
    row_year = row["Emission Year"]

    row_facility = [row["Facility Name"]]
    row_BU_and_type = [row["BU Name"], row["Type"]]
    row_type = [row["Type"]]
    row_BU = [row["BU Name"]]

    row_name_index = (row["Facility Name"], row["Emission Year"], row["Emission Month"])

    row_BU_and_type_index = (
        row["BU Name"],
        row["Type"],
        row["Emission Year"],
        row["Emission Month"],
    )
    row_type_index = (row["Type"], row["Emission Year"], row["Emission Month"])

    row_BU_index = (row["BU Name"], row["Emission Year"], row["Emission Month"])

    # data grouped together to be referenced in for loop
    list_of_indexes = [
        row_name_index,
        row_BU_and_type_index,
        row_type_index,
        row_BU_index,
    ]

    list_of_params = [row_facility, row_BU_and_type, row_type, row_BU]

    # reads from customer FTE selection excel
    type_list = FTE_selection["facility_type"].unique()
    activity_list = FTE_selection["activity"].unique()

    # hierarchy for choosing FTE and Capacity is the same as for choosing a rate, as show in documentation
    # first, checks if facility should use FTE or capacity
    if row["Sub Activity"] in activity_list and row_type in type_list:
        FTE_or_capacity_avgs = FTE_averages

        unindex=FTE_averages[0].reset_index()

        global_average=unindex.loc[:,'Capacity (m2)'].mean()

    else:
        FTE_or_capacity_avgs = capacity_averages

        unindex=capacity_averages[0].reset_index()

        global_average=unindex.loc[:,'Capacity (m2)'].mean()



    # runs through each parametre, in hierarchy order, to find mos suitable FTE or capacity
    for param, index, avg_df in zip(
        list_of_params, list_of_indexes, FTE_or_capacity_avgs
    ):

        param_with_year = tuple(param + [row_year])
        tupple_param = tuple(param)

        # checks if current parametre has a match in its corresponding FTE or Rates table
        if tupple_param in avg_df.index:

            # row month and year found in FTE df?
            if index in avg_df.index and avg_df[index] > 0:
                return avg_df[index]

            # if no month match, check if there is yearly avg

            elif (
                param_with_year in avg_df.index
                and avg_df.loc[param_with_year].mean() > 0
            ):
                return (avg_df.loc[param_with_year].mean())

                        # if no year match, use all yrs avg
            elif (avg_df.loc[tupple_param].mean()>0):
                return (avg_df.loc[tupple_param].mean())
    #if all else fails, returns a global average
    return (global_average)  


#TODO: TEST FUNCT,DELETE

# calcualtes rates for each row, following hiereachy order for avialable FTE or capacity
def get_CAPACITY_NAME_TEST(row, capacity_and_FTE_outputs, FTE_selection,custom_rate_selected):

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

    row_input = row["Input Quantity"]
    row_year = row["Emission Year"]

    row_facility = [row["Facility Name"]]
    row_BU_and_type = [row["BU Name"], row["Type"]]
    row_type = [row["Type"]]
    row_BU = [row["BU Name"]]

    row_name_index = (row["Facility Name"], row["Emission Year"], row["Emission Month"])

    row_BU_and_type_index = (
        row["BU Name"],
        row["Type"],
        row["Emission Year"],
        row["Emission Month"],
    )
    row_type_index = (row["Type"], row["Emission Year"], row["Emission Month"])

    row_BU_index = (row["BU Name"], row["Emission Year"], row["Emission Month"])

    # data grouped together to be referenced in for loop
    list_of_indexes = [
        row_name_index,
        row_BU_and_type_index,
        row_type_index,
        row_BU_index,
    ]

    list_of_params = [row_facility, row_BU_and_type, row_type, row_BU]

    # reads from customer FTE selection excel
    type_list = FTE_selection["facility_type"].unique()
    activity_list = FTE_selection["activity"].unique()

    # hierarchy for choosing FTE and Capacity is the same as for choosing a rate, as show in documentation
    # first, checks if facility should use FTE or capacity
    if row["Sub Activity"] in activity_list and row_type in type_list:
        FTE_or_capacity_avgs = FTE_averages
    else:
        FTE_or_capacity_avgs = capacity_averages

    # runs through each parametre, in hierarchy order, to find mos suitable FTE or capacity
    for param, index, avg_df in zip(
        list_of_params, list_of_indexes, FTE_or_capacity_avgs
    ):

        param_with_year = tuple(param + [row_year])
        tupple_param = tuple(param)

        # checks if current parametre has a match in its corresponding FTE or Rates table
        if tupple_param in avg_df.index:

            # row month and year found in FTE df?
            if index in avg_df.index and avg_df[index] > 0:
                return str(index)

            # if no month match, check if there is yearly avg

            elif (
                param_with_year in avg_df.index
                and avg_df.loc[param_with_year].mean() > 0
            ):
                return str(param_with_year)

            # if no year match, use all yrs avg
            # if no year match, use all yrs avg
            elif (avg_df.loc[tupple_param].mean()>0):
                return str(tupple_param)
    return ("global_average")  




# tags whether to use FTE or capacity for a facility
def get_rate_type(row, FTE_selection):

    # reads from customer FTE seleccition excel


    type_list = FTE_selection["facility_type"].unique()
    activity_list = FTE_selection["activity"].unique()
    

    # first, checks for specific subactivities
    if row["Sub Activity"] in activity_list and row["Type"] in type_list:
        return "FTE"
    # if its an activity type other than those in list
    else:
        return "capacity"
