from sources import *
import datetime
import calendar
import os


def generate_output(monthly_estimations_imported, daily_estimations_imported):

    # Get the path of the current script
    current_path = os.path.dirname(os.path.abspath(__file__))

    # Get the path of the parent directory and create export name for xlsx
    parent_path = os.path.dirname(current_path)
    file_path = os.path.join(parent_path, "Outputs/estimations_output.xlsx")

    print("\n Calculating Output...\n")

    ##daily_estimations_imported= [partial_data_index,partial_data_values]
    partial_index_imported = daily_estimations_imported[0]
    partial_values_imported = daily_estimations_imported[1]

    output_df = tabulate_output(
        monthly_estimations_imported, partial_index_imported, partial_values_imported
    )

    #print(output_df)

    output_df.to_excel(file_path, sheet_name="template_filler", index=False)

        # TODO: REMOVE THIS PART FOR QA OLY
    dt = dtale.show(output_df)
    # opens in browser
    dt.open_browser()
    # input("Press enter to exit")

    input("\n Press enter to generate Excel File\n")

    print("\n Excel has been generated, check the 'Outputs' folder ")


# this prepares the data fram that is to be exported
def tabulate_output(monthly_import, partial_index_imported, partial_values_imported):

    # [monthly_estimations_output,monthly_estimations_workable]
    monthly_source = monthly_import[0]
    end_day_mapping = monthly_import[1]
    first_day_mapping = monthly_import[2]

    # this first part creates export format for monthly data
    untagged_output = monthly_source[
        [
            "Facility Name",
            "Emission Month",
            "Emission Year",
            "estimation_for_period",
            "unit",
        ]
    ].copy()

    untagged_output = (
        untagged_output.groupby(
            ["Facility Name", "Emission Year", "Emission Month", "unit"]
        )["estimation_for_period"].sum()
    ).reset_index()
    untagged_output.insert(1, "reference_id", "")


    untagged_output["estimation_start_date"] = untagged_output.apply( lambda r: 
        get_start_date(r,first_day_mapping), axis="columns"
    )
    untagged_output["estimation_end_date"] = untagged_output.apply(
        lambda r: get_end_date(r, end_day_mapping), axis="columns"
    )

    untagged_output["Sub Activity"] = monthly_source.loc[0, "Sub Activity"]

    # this replaces monthly estimations for a given facility, onth, and year, with its match from partial data estiamtions

    if len(partial_values_imported.index) >0:
        # first, replaces estimation value
        untagged_output["estimation_for_period"] = untagged_output.apply(
            lambda r: replace_estimation_value(
                r, partial_index_imported, partial_values_imported
            ),
            axis="columns",
        )

        # then, replace estiamtion start date
        untagged_output["estimation_start_date"] = untagged_output.apply(
            lambda r: replace_estimation_start(
                r, partial_index_imported, partial_values_imported
            ),
            axis="columns",
        )

        # finally, replace estiamtion end date
        untagged_output["estimation_end_date"] = untagged_output.apply(
            lambda r: replace_estimation_end(
                r, partial_index_imported, partial_values_imported
            ),
            axis="columns",
        )

        # gets units for each row
        # untagged_output['unit']=untagged_output.apply(lambda r: get_units(r,monthly_source), axis='columns')
        #untagged_output = concat_remainging_partials(
         #   untagged_output, partial_values_imported
        #)


    untagged_output["estimation_for_period"] = round(untagged_output["estimation_for_period"],2)
    tagged_output = tag_and_reindex(untagged_output)
    return tagged_output


# applies the necesary tags depending on activity, and reindexes
def tag_and_reindex(untagged_source):

    activity = untagged_source.loc[0, "Sub Activity"]
    untagged_source = untagged_source.drop(
        columns=["Emission Year", "Emission Month", "Sub Activity"]
    )

    if activity == "Solid Waste Generation":
        untagged_source["waste_type"] = "Municipal Solid Waste -> Mixed"
        untagged_source["waste_treatment_type"] = "Landfilled"
        tagged_soruce = untagged_source.reindex(
            columns=[
                "Facility Name",
                "reference_id",
                "estimation_start_date",
                "estimation_end_date",
                "waste_type",
                "waste_treatment_type",
                "estimation_for_period",
                "unit",
            ]
        )
        return tagged_soruce


    elif activity == "Heating Use" or activity == "District Heating":
        untagged_source["fuel_type"] = "Gas.Natural Gas"
        tagged_soruce = untagged_source.reindex(
            columns=[
                "Facility Name",
                "reference_id",
                "estimation_start_date",
                "estimation_end_date",
                "fuel_type",
                "estimation_for_period",
                "unit",
            ]
        )
        return tagged_soruce

    # if there is nothing to tag, just reindexes
    else:
        tagged_soruce = untagged_source.reindex(
            columns=[
                "Facility Name",
                "reference_id",
                "estimation_start_date",
                "estimation_end_date",
                "estimation_for_period",
                "unit",
            ]
        )
        return tagged_soruce


# replaces monthly estimations with partial data estimations, if there are matching dates
def replace_estimation_value(row, partial_index_imported, partial_values_imported):

    row_name = row["Facility Name"]
    row_year = row["Emission Year"]
    row_month = row["Emission Month"]
    row_index = (row_name, row_year, row_month)

    # checks if the name-year-month combination existis within partial data estimations
    if (
        row_index in partial_index_imported.index
        and partial_index_imported[row_index] > 0
    ):

        # if they do, changes row to have same values as partial estimation
        facility_match = partial_values_imported.loc[
            (partial_values_imported["Facilty Name"] == row_name)
            & (partial_values_imported["Financial Year"] == row_year)
            & (partial_values_imported["Month"] == row_month)
        ]

        new_value = facility_match["estimation_for_period"]
        return new_value

    else:
        return row["estimation_for_period"]


def replace_estimation_start(row, partial_index_imported, partial_values_imported):

    row_name = row["Facility Name"]
    row_year = row["Emission Year"]
    row_month = row["Emission Month"]
    row_index = (row_name, row_year, row_month)

    # checks if the name-year-month combination existis within partial data estimations
    if (
        row_index in partial_index_imported.index
        and partial_index_imported[row_index] > 0
    ):

        # if they do, changes row to have same values as partial estimation
        facility_match = partial_values_imported.loc[
            (partial_values_imported["Facilty Name"] == row_name)
            & (partial_values_imported["Financial Year"] == row_year)
            & (partial_values_imported["Month"] == row_month)
        ]

        new_start = facility_match["estimation_start_date"]
        new_start = datetime.datetime.strptime(new_start, '%d/%m/%Y').strftime('%Y-%m-%d')
        return new_start

    else:
        return row["estimation_start_date"]


def replace_estimation_end(row, partial_index_imported, partial_values_imported):

    row_name = row["Facility Name"]
    row_year = row["Emission Year"]
    row_month = row["Emission Month"]
    row_index = (row_name, row_year, row_month)

    # checks if the name-year-month combination existis within partial data estimations
    if (
        row_index in partial_index_imported.index
        and partial_index_imported[row_index] > 0
    ):

        # if they do, changes row to have same values as partial estimation
        facility_match = partial_values_imported.loc[
            (partial_values_imported["Facilty Name"] == row_name)
            & (partial_values_imported["Financial Year"] == row_year)
            & (partial_values_imported["Month"] == row_month)
        ]

        new_end = facility_match["estimation_end_date"]
        new_end = datetime.datetime.strptime(new_end, '%d/%m/%Y').strftime('%Y-%m-%d')
        return new_end

    else:
        return row["estimation_end_date"]


# concatenates all partial data estimations that werent used to replace existing monthly values
def concat_remainging_partials(df, partial_values_df):

    # adjusts partial data to fit output format
    partials_output_format = partial_values_df[
        [
            "Facility Name",
            "Month",
            "Financial Year",
            "estimation_for_period",
            "estimation_start_date",
            "estimation_end_date",
        ]
    ].copy()
    partials_output_format = partials_output_format.rename(
        columns={"Month": "Emission Month", "Financial Year": "Emission Year"}
    )
    partials_output_format.insert(1, "reference_id", "")
    
    partials_output_format["unit"] = partials_output_format.apply(
        lambda r: get_units(r, partial_values_df), axis="columns"
    )
    partials_output_format["Sub Activity"] = df.loc[0, "Sub Activity"]

    # concatenates tables, removing any duplicated combinantio of name, month, and year
    combined_output = pd.concat(
        [df, partials_output_format], ignore_index=True
    ).drop_duplicates(subset=["Facility Name", "Emission Year", "Emission Month"])

    combined_output = combined_output.sort_values(
        by=["Facility Name", "Emission Month"]
    )
    return combined_output


# Matches units from existing dataset into new one, based on facility name
def get_units(row, base_df):

    row_name = row["Facility Name"]
    base_units = base_df.loc[base_df["Facility Name"] == row_name, "Input Unit"]
    selected_base_unit = base_units.iloc[0]
    return selected_base_unit


# gets the start date for a given row, and applies correct format
def get_start_date(row,first_day_mapping):

    row_year = row["Emission Year"]
    row_month = row["Emission Month"]
    row_name = row["Facility Name"]

    row_index = (row_name, row_year, row_month)

    # comopares when the data ends to maximum days in respective month
    # if number in df is lower than max days in given month, uses number of days in incomplete month

    mapped_days = first_day_mapping.loc[row_index].mean()

    max_days_in_month = calendar.monthrange(row_year, row_month)[1]



    if mapped_days != 1:
        selection = mapped_days
    else:
        selection = 1

    start_date = (
        str(int(selection))
        + "/"
        + str(row["Emission Month"])
        + "/"
        + str(row["Emission Year"])
    )

    formated_start_date = datetime.datetime.strptime(start_date, '%d/%m/%Y').strftime('%Y-%m-%d')
    return formated_start_date


# gets the end date for a given row, and applies correct format
def get_end_date(row, end_day_mapping):

    row_year = row["Emission Year"]
    row_month = row["Emission Month"]
    row_name = row["Facility Name"]

    row_index = (row_name, row_year, row_month)

    # comopares when the data ends to maximum days in respective month
    # if number in df is lower than max days in given month, uses number of days in incomplete month

    mapped_days = end_day_mapping.loc[row_index].mean()

    max_days_in_month = calendar.monthrange(row_year, row_month)[1]



    if mapped_days < max_days_in_month:
        selection = mapped_days
    else:
        selection = max_days_in_month

    end_date = (
        str(int(selection))
        + "/"
        + str(row["Emission Month"])
        + "/"
        + str(row["Emission Year"])
    )

    formated_start_end = datetime.datetime.strptime(end_date, '%d/%m/%Y').strftime('%Y-%m-%d')
    return formated_start_end