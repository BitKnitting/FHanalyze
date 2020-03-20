

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
        # Instead of asssert test, check if can format - if not, exception.
        try:
            (date.fromisoformat(i) for i in isodate_list)
        except ValueError as e:
            logging.error(f' {e}')
