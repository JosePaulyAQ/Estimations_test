from sources import *
import general_functions


def ingest_data(excluded_facilities, baseline_year):

    print("\n Calculating Capacity and FTE...\n")

    capacity_and_FTE_raw = capacity_and_FTE.merge(
        excluded_facilities[["Facility Name", "exclude_from_rates"]],
        on="Facility Name",
        how="left",
    )

    capacity_and_FTE_workable = general_functions.sanitize_data_general(
        capacity_and_FTE_raw, baseline_year
    )

    (
        capacity_name_avg,
        FTE_name_avg,
        capacity_BU_type_avg,
        FTE_BU_type_avg,
        capacity_type_avg,
        FTE_type_avg,
        capacity_BU_avg,
        FTE_BU_avg,
    ) = tabulate_cap_and_FTE(capacity_and_FTE_workable)


    #print( capacity_name_avg)
    #print( capacity_type_avg)
    # TODO: REMOVE THIS PART FOR QA OLY
    #dt = dtale.show(capacity_and_FTE_workable)
    # opens in browser
    #dt.open_browser()
    #input("Press enter to continue\n")

    return (
        capacity_name_avg,
        FTE_name_avg,
        capacity_BU_type_avg,
        FTE_BU_type_avg,
        capacity_type_avg,
        FTE_type_avg,
        capacity_BU_avg,
        FTE_BU_avg,
    )


# tabulates monthly avergaes per month and year, by facility, for both FTE and capacity
def tabulate_cap_and_FTE(workable_source):

    # facility name
    capacity_name_avg = workable_source[
        ["Year", "Month", "Capacity (m2)", "Facility Name"]
    ].copy()
    capacity_name_avg = capacity_name_avg.groupby(["Facility Name", "Year", "Month"])[
        "Capacity (m2)"
    ].mean()

    FTE_name_avg = workable_source[
        ["Year", "Month", "FTE", "Facility Name"]
    ].copy()
    FTE_name_avg = FTE_name_avg.groupby(
        ["Facility Name", "Year", "Month"]
    )["FTE"].mean()

    # BU and facility type
    capacity_BU_type_avg = workable_source[
        ["Year", "Month", "Capacity (m2)", "Type", "BU Name"]
    ].copy()
    capacity_BU_type_avg = capacity_BU_type_avg.groupby(
        ["BU Name", "Type", "Year", "Month"]
    )["Capacity (m2)"].mean()

    FTE_BU_type_avg = workable_source[
        ["Year", "Month", "FTE", "Type", "BU Name"]
    ].copy()
    FTE_BU_type_avg = FTE_BU_type_avg.groupby(["BU Name", "Type", "Year", "Month"])[
        "FTE"
    ].mean()

    # facility type
    capacity_type_avg = workable_source[
        ["Year", "Month", "Capacity (m2)", "Type"]
    ].copy()
    capacity_type_avg = capacity_type_avg.groupby(["Type", "Year", "Month"])[
        "Capacity (m2)"
    ].mean()

    FTE_type_avg = workable_source[["Year", "Month", "FTE", "Type"]].copy()
    FTE_type_avg = FTE_type_avg.groupby(["Type", "Year", "Month"])["FTE"].mean()

    # BU
    capacity_BU_avg = workable_source[
        ["Year", "Month", "Capacity (m2)", "BU Name"]
    ].copy()
    capacity_BU_avg = capacity_BU_avg.groupby(["BU Name", "Year", "Month"])[
        "Capacity (m2)"
    ].mean()

    FTE_BU_avg = workable_source[["Year", "Month", "FTE", "BU Name"]].copy()
    FTE_BU_avg = FTE_BU_avg.groupby(["BU Name", "Year", "Month"])["FTE"].mean()

    return [
        capacity_name_avg,
        FTE_name_avg,
        capacity_BU_type_avg,
        FTE_BU_type_avg,
        capacity_type_avg,
        FTE_type_avg,
        capacity_BU_avg,
        FTE_BU_avg,
    ]
