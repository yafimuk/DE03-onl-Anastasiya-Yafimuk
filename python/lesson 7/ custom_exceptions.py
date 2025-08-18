class NegativeNumberError(Exception):
    """Custom exception to be raised when a specific condition is met."""
    pass

def check_value(number):
    if number < 0:
        raise NegativeNumberError("Вы ввели отрицательное число!")
    

try:
    check_value(-5)  # This will raise the custom exception
except NegativeNumberError as e:
    print(f"Custom Exception Caught: {e}")  




class InvalidAgeError(Exception):
    def __init__(self, age):
        self.age = age

def validate_age(age):
    if age < 0 or age > 120:
        raise InvalidAgeError(age)
    
try:
    validate_age(150)  # This will raise the custom exception
except InvalidAgeError as e:
    print(f"Custom Exception Caught: Invalid age {e.age}. Age must be between 0 and 120.")    