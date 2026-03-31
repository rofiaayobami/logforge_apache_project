import re

LOG_PATTERN = re.compile(
    r'(?P<ip>\S+) .* '
    r'\[(?P<timestamp>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) (?P<protocol>[^"]+)" '
    r'(?P<status>\d{3}) (?P<bytes>\S+) '
    r'"(?P<referrer>[^"]*)" "(?P<agent>[^"]*)"'
)

def parse_log_line(line: str):
    """
    Parses a single Apache log line.
    Returns a dictionary of fields or None if malformed.
    """
    match = LOG_PATTERN.match(line)

    if not match:
        return None

    data = match.groupdict()

    # Convert numeric fields
    data["status"] = int(data["status"])

    if data["bytes"].isdigit():
        data["bytes"] = int(data["bytes"])
    else:
        data["bytes"] = 0

    return data
