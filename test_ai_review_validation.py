def unsafe_divide(x, y):
    print(f"dividing {x} by {y}")
    return x / y


def build_query(table_name, value):
    return f"select * from {table_name} where k = '{value}'"
