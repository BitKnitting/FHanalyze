import sys
import logging
logger = logging.getLogger(__name__)


class Error(Exception):
    """ Base class for the exceptions in this module. """
    pass

    def __init__(self, message):
        self.message = message


def handle_exception(e):
    """Function all modules share to handled exceptions.
    Currently error strings (e) are put into the log file as
    an exception.

    :param e: Error message.

    """

    logger.exception(f'Exception...{e}')
    sys.exit()
