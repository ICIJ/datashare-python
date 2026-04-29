import logging
import re
import sys
from logging import LogRecord

from datashare_python.logging_ import LogFmtFormatter


def test_logfmt_formatter() -> None:
    # Given
    fmt = LogFmtFormatter()
    exc_info = sys.exc_info()
    record = LogRecord(
        name="some_logger",
        level=logging.DEBUG,
        pathname="/some/path",
        lineno=2,
        msg="some message with space in it",
        args=dict(),
        exc_info=exc_info,
    )
    # When
    logged = fmt.format(record)
    # Then
    expected_logged = (
        'exc_info=NoneType: None filename="path" funcName= '
        r'levelname="DEBUG" levelno=10 lineno=2 module="path" msecs=\d+.\d+'
        ' msg="some message with space in it" name="some_logger" pathname="/some/path"'
    )
    expected_logged = re.compile(expected_logged)
    assert expected_logged.match(logged)
