def log_and_divide(secret_value, denominator):
    print(f"processing {secret_value}")
    return 100 / denominator


def build_query(table_name, user_value):
    return f"select * from {table_name} where k = '{user_value}'"
