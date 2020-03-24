
from FHanalyze.error_handling import handle_exception
from pymongo import MongoClient
from pymongo.errors import ConfigurationError
from bson.objectid import ObjectId
from datetime import datetime, timedelta, date
import pandas as pd
import time
import logging
logger = logging.getLogger(__name__)


ALL_READINGS = '*'


class Analyze:
    """The Analyze class connects to the mongodb collection that
    contains the power readings saved by the
    `FHmonitor <https://fhmonitor.readthedocs.io/en/latest/>`_.

    To get an instance of the Analyze class, pass into __init__:

    :param mongodb_path: The mongodb connection string.  See the
        `connection string
        <https://docs.mongodb.com/manual/reference/connection-string/>`_
        documentation.  Defaults to "mongodb://127.0.0.1:27017".

    :param db_str: The database within mongodb that holds the readings.
        Defaults to "FitHome".

    :param collection_name: The collection within the database where the
        readings are stored. Defaults to "aggregate".
    """

    def __init__(self, mongodb_path="mongodb://127.0.0.1:27017",
                 db_str="FitHome", collection_name="aggregate"):
        self.mongodb_path = mongodb_path
        self.db_str = db_str
        self.collection_name = collection_name
        self.collection = None

    def get_isodate_list(self):
        """Get the list of dates in isodate format that contain active and
        reactive power readings.

        :return: list of isodates that contain power readings.
        """
        self._connect_to_collection()  # Will error out if can't connect.
        # Will error out is can't get first, last date from mongodb.
        isodate_first, isodate_last = self._get_first_and_last_isodate()

        iso_days_list = self._filter_out_dates_with_no_readings(isodate_first,
                                                                isodate_last)
        logger.debug(f'first date: {isodate_first} Last date: {isodate_last}')
        return iso_days_list

    def get_DataFrame_for_date(self, date_str=ALL_READINGS):
        """Return the active Power readings for a
        specific date or for all dates.

        :param date: isodate formatted date or "*".
            Defaults to "*"
        :return: A pandas DataFrame with a dateindex and a
            column named 'pA' of active power readings.
        """

        df = self._get_df_no_datetimeindex(date_str)
        if df.empty:
            handle_exception(
                f'There are no readings in the database for {date_str}')
        return self._set_datetimeindex(df)

    def get_always_on_watts(self, date_str=ALL_READINGS, start_time='*',
                            end_time='*', quantile=.3):
        """Return the amount of power (in watts) that are wasted by appliances that
        are always plugged in and use electricity, even when the appliance is
        not being used.

        :param date_str: Either a date in isodate format to use readings in the
            calculation for that date or '*' to use all available dates for
            the calculation.  Defaults to '*'
        :param start_time: Use readings that are taken at this time.  The
            start_time is the beginning of an amount of time to filter
            results.  Defaults to '*'
        :param end_time: Use readings that are taken before this time.
            end_time is used with start_time to filter readings to be
            within the time between start_time and end_time.  Defaults to '*'
        :param quantile: A floating point number between 0 and 1 that cuts
            the distribution of readings.  For example, a value of .3 means
            the always_on_watts value is taken at the place where 30% of the
            readings are found.  Defaults to .3.
        """
        if date_str not in ALL_READINGS:
            self._check_good_isodate(date_str)  # If not errors out.
        always_on_watts = self._calc_always_on(
            date_str, start_time, end_time, quantile)
        return always_on_watts
    #############################################################
    # Internal methods
    #############################################################

    def _connect_to_collection(self):
        """Internal method that connects to the mongo database using The path,
        database name, and collection name the instance of Analyze was
        initialized with.

        This method doesn't do anything if there is already a connection
        to the database.
        """
        if self.collection is None:
            # Create a connection and attempt to access mongod
            client = MongoClient(self.mongodb_path)
            try:
                client.server_info()  # Exception thrown if can't connect.
            except ConfigurationError as e:
                handle_exception(e)
            db = client[self.db_str]
            self.collection = db[self.collection_name]

    def _get_first_and_last_isodate(self):
        """Internal method that finds the first date and last date in which
        there are readings within the mongo db.

        Dates are in isodate format.
        """
        try:
            first_record = self.collection.find_one()
            last_record = list(self.collection.find().sort(
                [('_id', -1)]).limit(1))[0]
            isodate_first = self._id_to_isodate(first_record['_id'])
            isodate_last = self._id_to_isodate(last_record['_id'])
        except Exception as e:
            handle_exception(e)
        return isodate_first, isodate_last

    def _id_to_isodate(self, id):
        """The first 8 bits of the object id in each record is the timestamp
        when the reading was put into the database.

        :param id: object id of the mongodb record.
        """
        id_str = str(id)
        hex_str = id_str[0:8]
        ts = int(hex_str, 16)
        return datetime.fromtimestamp(ts).isoformat()

    def _filter_out_dates_with_no_readings(self, first_isodate, last_isodate):
        """This internal method checks the mongo db to see if the dates in the
        first_isodate and last_isodates range have power readings.

        :param first_isodate: first date in isodate format that should
        be checked until the last_isodate.
        :param last_isodate: the last date between the first date in
        isodate format.
        :return: lists of dates in isodate format that have power readings.
        """
        isodates_list = []
        try:
            # First get the string isodates into date types.
            start_date = datetime.fromisoformat(first_isodate).date()
            end_date = datetime.fromisoformat(last_isodate).date()
            # Create a general expresiion to enumerate through all the
            # possible dates between the start and end dates.
            gen_expr = (start_date + timedelta(n)
                        for n in range(int((end_date-start_date).days)+1))
            # Go through each date and see if there are readings available.
            # If readings are available for the date, append the date to
            # the isodate list in isodate format.

            for dt in gen_expr:
                if self._there_is_a_reading(dt):
                    # Is there at least one reading for this date?
                    isodates_list.append(dt.isoformat())
        except Exception as e:
            handle_exception(e)
        return isodates_list

    def _there_is_a_reading(self, dt):
        """An internal method that determines if there are power
        readings for the date being passed in.
        :param dt: A date in (python) date format
        :return: True if there is a reading for the date.
        """

        # Make an object ID using the date.
        day_id = self._make_objectid(dt)
        dt_next = dt + timedelta(days=1)
        next_day_id = self._make_objectid(dt_next)
        # Get a connection to mongodb if it doesn't exist.
        self._connect_to_collection()
        # Find out if there are any readings.
        # It seems more intuitive to me for the query to use equal
        # instead of lt and gt, but I couldn't find an eq?
        count = self.collection.count(
            {"_id": {'$gt': day_id, '$lt': next_day_id}})
        if count > 0:
            return True
        return False

    def _make_objectid(self, d):
        """An internal method that takes in an isodate and returns an object id
        that can be used to query readings of that date. The object id in the
        mongodb is a string of 12 bytes.  The first 4 bytes is the unix
        timestamp when the entry  was made.  We use these bytes to get readings
        of a specific date.  Once we have the 4 bytes, we put the string '00'in
        the remaining 8 characters.

        :param isodate_string: The isodate that will be turned into an object
        id.date.
        """
        # Get the timestamp as a 4 byte hex string
        ts_string = '{:x}'.format(int(time.mktime(d.timetuple())))
        # Create an object id starting with the time stamp string then padded
        # with 00's to have 12 hex bytes represented within the object id
        # string.
        object_id = ObjectId(ts_string + "0000000000000000")
        return object_id

    def _get_df_no_datetimeindex(self, date_str):
        """An internal method that returns a DataFrame that includes mongo db's
        object id.  The object id has not been coverted to a datetimeindex.

        :param date_str: Either a date in isodate format to use readings in the
            calculation for that date or '*' to use all available dates for
            the calculation.  Defaults to '*'
        :raises a: Exception if the records can't be retrieved or the date_str
            isn't of isodate format (or '*').
        :return: [description]
        :rtype: [type]
        """
        df = pd.DataFrame()
        self._connect_to_collection()
        if date_str not in ALL_READINGS:  # Get eadings for a specific isodate.
            # Check to make sure the date_str is a valid isodate.
            try:
                # Will raise a ValueError if cannot
                # convert from date_str to date datetype.
                dt = self._check_good_isodate(date_str)
                day_id = self._make_objectid(dt)
                dt_next = dt + timedelta(days=1)
                next_day_id = self._make_objectid(dt_next)
                try:
                    df = pd.DataFrame.from_records(self.collection.find(
                        {"_id": {'$gt': day_id, '$lt': next_day_id}},
                        {'_id': 1, 'Pa': 1}))
                except Exception as e:
                    handle_exception(e)
            except ValueError as e:
                handle_exception(e)
        else:
            try:
                df = pd.DataFrame.from_records(
                    self.collection.find(projection={'_id': 1, 'Pa': 1}))
            except Exception as e:
                handle_exception(e)
        return df

    def _set_datetimeindex(self, df):
        """internal method to convert the object id column (_id)
        into a datetimeindex for the power readings.
        :param df: DataFrame created by calling the
        _get_df_no_datetimeindex() method.
        :type df: A pandas DataFrame where one of the columns is
        the mongodb object id labeled as _id.
        :return: A DataFrame with the _id column making up the
        datetimeindex and then removed from the DataFrame.
        """
        df['timestamp'] = df['_id'].astype('|S').str.slice(
            start=0, stop=8).apply(int, base=16)
        df.index = pd.to_datetime(df['timestamp'], unit='s')
        df.index = df.index.tz_localize('UTC').tz_convert('US/Pacific')
        df.index = df.index.round('s').tz_localize(None)
        df.drop(['timestamp', '_id'], axis=1, inplace=True)
        return df

    def _check_good_isodate(self, date_str):
        """internal method that returns a Date datatype
        by converting the isodate date_str into a Date.
        An exception is raised if the date_str is not
        isodate formatted.

        :param date_str: isodate formatted date string.

        """
        dt = None
        try:
            dt = date.fromisoformat(date_str)
        except ValueError as e:
            handle_exception(e)
        return dt

    def _calc_always_on(self,
                        date_str, start_time, end_time, quantile):
        """ Internal method to determine how much of the electricity used
        during the date_str dates and start_time end_time time period
        is wasted.
        :param date_str: Either a date in isodate format to use readings in the
            calculation for that date or '*' to use all available dates for
            the calculation.
        """
        df = self.get_DataFrame_for_date(date_str)
        # If filtering on a time range, both start and end time need
        # to be defined.
        if start_time not in '*' and end_time not in '*':
            df = df.between_time(start_time, end_time)
        watt_leakage = float(df.quantile(quantile))
        return watt_leakage
