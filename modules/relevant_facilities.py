from sources import *


def run_dtale():
    # Uses dtale so the user can select relevant facilities
    dt = dtale.show(facility_relevancy, port=40001)
    # opens in browser
    dt.open_browser()

    input(
        "\n Press Enter when you are done marking facilities that you want to estimate for...\n"
    )
    temp_selected_facilities = dt.data

    # Uses dtale so the user can select excluded facilities
    dp = dtale.show(facility_exclusion, port=4002)
    # opens in browser
    dp.open_browser()

    input(
        "\n Press Enter when you are done marking facilities that you DONT want to use to calculate estimations. ...\n"
    )
    temp_excluded_facilities = dp.data

    return temp_selected_facilities, temp_excluded_facilities
