class MyCalculator:
    def __init__(self):
        self.last_result = None
        self.history = []   # list to store all calculations

    def add_numbers(self, a, b):
        self.last_result = a + b
        self.history.append(f"{a} + {b} = {self.last_result}")
        return self.last_result
 
    def subtract_numbers(self, a, b):
        self.last_result = a - b
        self.history.append(f"{a} - {b} = {self.last_result}")
        return self.last_result

    def multiply_numbers(self, a, b):
        self.last_result = a * b
        self.history.append(f"{a} * {b} = {self.last_result}")
        return self.last_result

    def divide_numbers(self, a, b):
        try:
            self.last_result = a / b
            self.history.append(f"{a} / {b} = {self.last_result}")
            return self.last_result
        except ZeroDivisionError:
            error_msg = "Error: Division by zero not allowed!"
            self.history.append(f"{a} / {b} = {error_msg}")
            return error_msg

    def square_last_result(self):
        if self.last_result is None:
            return "No calculation done yet!"
        squared = self.last_result ** 2
        self.history.append(f"({self.last_result})² = {squared}")
        self.last_result = squared
        return squared

    def show_history(self):
        return "\n".join(self.history) if self.history else "No calculations yet."

    def clear_history(self):
        self.history = []
        self.last_result = None
        return "History and last result cleared."
