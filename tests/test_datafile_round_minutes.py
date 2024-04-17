import pytest
from clams_convert.datafile import round_minutes
from datetime import datetime, time

def test_round_minutes_up():
    assert round_minutes(datetime(2000,1,12,18, 00, 50)) == datetime(2000,1,12,18, 1)

def test_round_minutes_down():
    assert round_minutes(datetime(2000,1,12,18, 00, 50), how = "down") == datetime(2000,1,12,18, 0)

def test_round_minutes_none():
    assert round_minutes(datetime(2000,1,12,18, 00, 50), how = "gg") == None

def test_round_minutes_incorrect_type():
    with pytest.raises(AttributeError):
        round_minutes("foobar")

def test_round_minutes_time():
    with pytest.raises(TypeError):
        round_minutes(time(18,2,2))