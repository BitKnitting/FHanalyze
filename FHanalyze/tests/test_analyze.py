

"""Tests for `analyze` package."""

from FHanalyze.analyze import Analyze
from datetime import date
import pytest
import logging
logging.basicConfig(level=logging.DEBUG)


@pytest.fixture(scope='module')
def analyze_instance():
    analyze_instance = Analyze()
    return analyze_instance


def test_get_list_of_dates_with_readings(analyze_instance):
    """checks the isodates for reading dates are valid.

    :param analyze_instance: An instance of the Analyze class.

    """
    isodate_list = []
    isodate_list = analyze_instance.get_isodate_list()
    # There may be no readings.  If this is the case, the len of list = 0.
    if (len(isodate_list) > 0):
        # Instead of asssert test, check if can create a date from the
        # string.  If possible, the string is in isodate format.
        try:
            [date.fromisoformat(date_str) for date_str in isodate_list]
        except ValueError as e:
            logging.error(f' {e}')


# Pass in a date strig that is not isodate formatted.
# This test fails.


def test_bad_date_to_get_DataFrame(analyze_instance):

    bad_date_str = "02-13-2020"
    analyze_instance.get_DataFrame_for_date(bad_date_str)
    # Not using an assert.  Try/except error handling.


def test_date_not_in_readings(analyze_instance):
    date_not_in_readings_str = "2019-12-30"
    analyze_instance.get_DataFrame_for_date(date_not_in_readings_str)
    # Not using an assert.  Try/except error handling.


def test_readings_for_date(analyze_instance):
    # Assumes above test passed
    isodate_list = analyze_instance.get_isodate_list()
    logging.debug(f'List of isodates: {isodate_list}')
    logging.debug(f'Date being used to query readings: {isodate_list[0]}')
    df = analyze_instance.get_DataFrame_for_date(isodate_list[0])
    logging.debug(df.describe())
    assert not df.empty

# Getting all the readings can take a long time.


def test_get_all_readings(analyze_instance):
    df = analyze_instance.get_DataFrame_for_date("*")
    print(df.describe())
    logging.debug(df.describe())
    assert not df.empty
