### This is an example method for learning tasks

### Example task: Implement a method `add_two_numbers` which will take two numbers and add them together

# MY METHOD
def add_two_numbers(number1: int, number2: int):
    return number1 + number2


## TESTS
def test_add_two_numbers():
    result = add_two_numbers(10, 1)
    
    assert result == 10
    
    print('pass')


# CALL THE TEST METHODS
test_add_two_numbers()    