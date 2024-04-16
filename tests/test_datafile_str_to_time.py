import pytest
from clams_convert.datafile import str_to_time
from clams_convert.datafile import str_to_time
from datetime import timedelta, datetime, time

def test_str_to_time():
    assert str_to_time("18:00:00") == time(18, 00, 00)

def test_raises_exception_on_non_int():
    with pytest.raises(ValueError):
        str_to_time("a")

def test_raises_exception_on_incorrect_sep():
    with pytest.raises(ValueError):
        str_to_time("15-25-32")

def test_raises_exception_on_incorrect_time():
    with pytest.raises(ValueError):
        str_to_time("15:68:32")
