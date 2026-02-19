from python_code import add, subtract, multiply, divide, number_to_string, string_to_number, number_to_float, float_to_number

def test_add():
    assert add(1, 2) == 3

def test_subtract():
    assert subtract(1, 2) == -1

def test_multiply():
    assert multiply(1, 2) == 2

def test_divide():
    assert divide(1, 2) == 0.5