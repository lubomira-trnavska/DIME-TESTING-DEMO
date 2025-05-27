import pytest
from methods import example_method

## test with two positive numbers
def test_add_two_numbers():
    result = example_method.add_two_numbers(10, 1)

    assert result == 11

## test with negative numbers
def test_add_two_numbers_negative():
    result = example_method.add_two_numbers(-10, -5)

    assert result == -15

## test raising exception if the input is not a nuber
def test_add_two_numbers_invalid_args():
    with pytest.raises(TypeError):
        example_method.add_two_numbers('11', 7)
