import pytest
from clams_convert.datafile import str_to_time
from datetime import time

def test_str_to_time():
    assert str_to_time("18:00:00") == time(18, 00, 00)

def test_non_int():
    with pytest.raises(ValueError):
        str_to_time("foobar")

def test_incorrect_hour_value():
    with pytest.raises(ValueError):
        str_to_time("25:68:32")

def test_incorrect_minute_value():
    with pytest.raises(ValueError):
        str_to_time("15:68:32")

def test_incorrect_second_value():
    with pytest.raises(ValueError):
        str_to_time("15:68:32")