
from FHanalyze.error_handling import handle_exception
from pymongo import MongoClient
from pymongo.errors import ConfigurationError
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import time
import logging
logger = logging.getLogger(__name__)

"""Query the Raspberry Pi's mongo db for active and reactive power readings
stored by calls to the FHmonitor pypi package.
"""
ALL_READINGS = '*'


class Analyze:
    """The Analyze class connects to the mongodb collection storing the active
    and reactive power readings saved by FHmonitor.

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

    def _connect_to_collection(self):
        """Internal method that connects to the mongo database using The path,
        database name, and collection name the instance of Analyze was
        initialized with.

        This method doesn't do anything if there is already a connection
        to the database.
        """
        if self.collection is None:
            # Create a connection and attempt to access mongod
            pass

        client = MongoClient(self.mongodb_path)
        try:
            client.server_info()  # Exception thrown if can't connect.
        except ConfigurationError as e:
            handle_exception(e)
        db = client[self.db_str]
        self.collection = db[self.collection_name]


    def get_leakage_amount(self, date='*', start_time='*', end_time='*',
                           quantile=.3):
        """[summary]

        :param date: [description], defaults to '*'
        :type date: str, optional
        :param start_time: [description], defaults to '*'
        :type start_time: str, optional
        :param end_time: [description], defaults to '*'
        :type end_time: str, optional
        :param quantile: [description], defaults to .3
        :type quantile: float, optional
        """
        pass

    def get_DataFrame_for_date(self, date=ALL_READINGS):
        pass

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
