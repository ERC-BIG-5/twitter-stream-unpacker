import io


def efficient_line_iterator(data: bytes, encoding: str = "utf-8"):
    """
    Create an efficient iterator for reading lines from byte data.

    :param data: Byte string containing the data
    :param encoding: Encoding of the data (default is UTF-8)
    :return: Iterator yielding lines as strings
    """
    return io.TextIOWrapper(io.BytesIO(data), encoding=encoding)


# Example usage
jsonl_str_data = b"""{"key": "value1"}
{"key": "value2"}
{"key": "value3"}
{"key": "value4"}
{"key": "value5"}
...
{"key": "value5000"}"""

# Create the iterator
line_iterator = efficient_line_iterator(jsonl_str_data)

# Read specific lines
for i, line in enumerate(line_iterator, start=1):
    print(f"Line {i}: {line.strip()}")
    if i == 8:  # Stop after reading the 8th line
        break


# If you need to access a specific line directly
def get_specific_line(data: bytes, line_number: int, encoding: str = "utf-8") -> str:
    with efficient_line_iterator(data, encoding) as f:
        for i, line in enumerate(f, start=1):
            if i == line_number:
                return line.strip()
    raise ValueError(f"Line {line_number} not found in the data")


# Example: Get the 5th line
try:
    fifth_line = get_specific_line(jsonl_str_data, 5)
    print(f"5th line: {fifth_line}")
except ValueError as e:
    print(e)