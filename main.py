import sys
from nicegui import ui

# reference to modules folder
sys.path.insert(0, "./modules")

import monthly_calculations
import capacity_and_FTE_calculations
import rates
import daily_calculations
import outputs
import relevant_facilities
from sources import *

# Takes user input for baseline year:
# is_baseline=input(" Are You estimating for the Baseline Year? (Y/N): ")
# if is_baseline == 'Y':
#   is_baseline=True
# else:
#   is_baseline=False

# there is no difference for calcs, just use it to flag if its baseline, and takes all existen facilities as relevant
is_baseline = True


# Captures input on which facilities to estiamte for, and which rates to ignore
selected_facilities, excluded_facilities = relevant_facilities.run_dtale()


# TODO: Change this to use REGEX

# if it is baseline, dont ask about getting baseline year
available_years_baseline = (
    rates_and_facilities["Emission Year"]
    .replace(",", "", regex=True)
    .unique()
    .astype(int)
)
print(
    " Please select a year to be the baseline, the avaialble years are:\n",
    available_years_baseline,
)
baseline_year = int(input(" \n Your selection : ").replace(",", "").replace(" ", ""))





custom_rate_selected = input("\nDo you wish to use a custom rate for the estimations? Y/N:\n")


custom_rate_selected = True if custom_rate_selected =="Y" else False


# [ capacity_name_avg, FTE_name_avg, capacity_BU_type_avg, FTE_BU_type_avg, capacity_type_avg,
# FTE_type_avg, capacity_BU_avg, FTE_BU_avg]

capacity_and_FTE_outputs = capacity_and_FTE_calculations.ingest_data(
    excluded_facilities, baseline_year
)

# [rates_workable,facility_rates,BU_and_type_rates,type_rates,BU_rates,activity_rates,facility_units,BU_and_type_units,type_units,BU_units,activity_units]
rates_outputs = rates.ingest_data(
    excluded_facilities, baseline_year, capacity_and_FTE_outputs,custom_rate_selected
)

# [partial_data_index,partial_data_values]
daily_outputs = daily_calculations.ingest_data(excluded_facilities, baseline_year)

# [monthly_estimations_output,end_day_mapping,first_day_mapping]
monthly_outputs = monthly_calculations.ingest_data(
    selected_facilities, baseline_year, capacity_and_FTE_outputs, rates_outputs,custom_rate_selected
)

outputs.generate_output(monthly_outputs, 
daily_outputs)
