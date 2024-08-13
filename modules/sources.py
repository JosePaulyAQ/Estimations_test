import pandas as pd
import numpy as np
import dtale
import chardet
import re
import glob

pd.options.mode.chained_assignment = None  # default='warn'

def open_file_by_regex(pattern):

    #name mapping:
    # facilities_emissions_coverage_per_month == monthly_estimations
    # monthly_consumptions == rates_and_facilities
    # facilities_capacity_and_fte == capacity_and_FTE
    # daily_emissons_coverage == daily_estimations
    # Use glob to find all files in the directory
    files = glob.glob("./data_sources/*.csv")

    matching_files=[]
    # Use regex to match files based on the provided pattern
    for pattern in patterns_list:
        temp = [file for file in files if pattern.search(file)]
        matching_files.append(temp[0])
 

    #ORDERING IS IMPORTANT
    return matching_files
    
#gets the individual encoding for each read csv
#IMPORTANT, since files seem to have differrent encondings
def get_encoding(matching_files,file_index_list):

    #name mapping:
    # facilities_emissions_coverage_per_month == monthly_estimations
    # monthly_consumptions == rates_and_facilities
    # facilities_capacity_and_fte == capacity_and_FTE
    # daily_emissons_coverage == daily_estimations

    monthly_filename_index = file_index_list[0]
    monthly_estimations_rawdata = open(matching_files[monthly_filename_index], "rb").read()
    #monthly_estimations_rawdata = open("./data_sources/monthly_estimations.csv", "rb").read()
    monthly_estimations_result = chardet.detect(monthly_estimations_rawdata)
    monthly_estimations_encoding = monthly_estimations_result["encoding"]

    daily_filename_index = file_index_list[1]
    daily_estimations_rawdata = open(matching_files[daily_filename_index], "rb").read()
    #daily_estimations_rawdata = open("./data_sources/daily_estimations.csv", "rb").read()
    daily_estimations_result = chardet.detect(daily_estimations_rawdata)
    daily_estimations_encoding = daily_estimations_result["encoding"]


    rates_filename_index = file_index_list[2]
    rates_and_facilities_rawdata = open(matching_files[rates_filename_index], "rb").read()
    #rates_and_facilities_rawdata = open("./data_sources/rates_and_facilities.csv", "rb").read()
    rates_and_facilities_result = chardet.detect(rates_and_facilities_rawdata)
    rates_and_facilities_encoding = rates_and_facilities_result["encoding"]


    capacity_filename_index = file_index_list[3]
    capacity_and_FTE_rawdata = open(matching_files[capacity_filename_index], "rb").read()
    #capacity_and_FTE_rawdata = open("./data_sources/capacity_and_FTE.csv", "rb").read()
    capacity_and_FTE_result = chardet.detect(capacity_and_FTE_rawdata)
    capacity_and_FTE_encoding = capacity_and_FTE_result["encoding"]

    return (monthly_estimations_encoding,daily_estimations_encoding,rates_and_facilities_encoding,capacity_and_FTE_encoding)



#this function returns the index value in a list of a specific string
def find_string_index(substring, list):
    ids = []

    for index_position, string in enumerate(list):
        if substring in string:
            ids.append(index_position)

    return ids[0]

def read_files(matching_files,file_index_list,encoding_list):

    monthly_index = file_index_list[0]
    daily_index = file_index_list[1]
    rates_index = file_index_list[2]
    capacity_index = file_index_list[3]

    monthly_estimations = pd.read_csv(
        matching_files[monthly_index], encoding=encoding_list[0]
    )
    daily_estimations = pd.read_csv(
        matching_files[daily_index], encoding=encoding_list[1]
    )
    rates_and_facilities = pd.read_csv(
        matching_files[rates_index], encoding=encoding_list[2]
    )
    capacity_and_FTE = pd.read_csv(
        matching_files[capacity_index], encoding=encoding_list[3]
    )

    #This is the file that controls which facilities to use FTE for
    selected_FTE_facilities_and_activities = pd.read_excel(
        "./data_sources/FTE_facilities_and_activities.xlsx"
    )

    return(monthly_estimations,daily_estimations,rates_and_facilities,capacity_and_FTE,selected_FTE_facilities_and_activities)


# Define the pattern to match files
monthly_pattern = re.compile(r"facilities_emissions_coverage_per_month.*\.csv")
rates_pattern = re.compile(r"monthly_consumptions.*\.csv")
capacity_pattern = re.compile(r"facilities_capacity_and_fte.*\.csv")
daily_pattern = re.compile(r"daily_emissons_coverage.*\.csv")

patterns_list=[monthly_pattern,rates_pattern,capacity_pattern,daily_pattern]

#find all files matching the patterns above, which come from the filenames in metabase
matching_files = open_file_by_regex(patterns_list)

monthly_filename_index = find_string_index('facilities_emissions_coverage_per_month',matching_files)
daily_filename_index = find_string_index('daily_emissons_coverage',matching_files)
rates_filename_index = find_string_index('monthly_consumptions',matching_files)
capacity_filename_index = find_string_index('facilities_capacity_and_fte',matching_files)

#orders indexes for eahc expected file in a know order, so they can be matched to needed varaibles consistently
file_index_list=[monthly_filename_index,daily_filename_index,rates_filename_index,capacity_filename_index]


#gets necesry encoding for each file
#[monthly_estimations_encoding,daily_estimations_encoding,rates_and_facilities_encoding,capacity_and_FTE_encoding]
encoding_list = get_encoding(matching_files,file_index_list)


monthly_estimations,daily_estimations,rates_and_facilities,capacity_and_FTE,selected_FTE_facilities_and_activities=read_files(matching_files,file_index_list,encoding_list)


# Creates table for the user to select relevant BUs for calculation
facility_relevancy = monthly_estimations[
    ["Activity", "Sub Activity", "BU Name", "Facility Name"]
].copy()
facility_relevancy["is_relevant"] = True
facility_relevancy = (
    facility_relevancy.groupby("Facility Name")
    .first()
    .sort_values(by=["BU Name", "Facility Name"])
    .reset_index()
)


# Creates table for the user to select relevant BUs for calculation
facility_exclusion = rates_and_facilities[
    ["Activity", "Sub Activity", "BU Name", "Facility Name"]
].copy()
facility_exclusion["exclude_from_rates"] = False
facility_exclusion = (
    facility_exclusion.groupby("Facility Name")
    .first()
    .sort_values(by=["BU Name", "Facility Name"])
    .reset_index()
)
