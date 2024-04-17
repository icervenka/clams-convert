import pytest
from clams_convert.datafile import freq_to_seconds
from datetime import timedelta

def test_freq_to_seconds():
    assert freq_to_seconds(timedelta(minutes=60)) == 3600

def test_non_incorrect_type():
    with pytest.raises(AttributeError):
        freq_to_seconds("foobar")
