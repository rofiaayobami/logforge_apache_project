import argparse
from pathlib import Path

from parser import parse_log_line
from database import (
    get_connection,
    create_tables,
    insert_log,
    insert_error
)

import hashlib

def generate_log_id(parsed):
    # combine unique fields to make hash
    unique_str = f"{parsed['ip']}-{parsed['timestamp']}-{parsed['path']}"
    return hashlib.md5(unique_str.encode()).hexdigest()


def extract_logs(input_path):
    """
    Read all .log or .txt files from a directory
    """
    log_files = Path(input_path).glob("*.txt")
    return log_files

def process_logs(log_files):
    """
    Parse log lines and load them into the database
    """
    conn = get_connection()
    create_tables(conn)

    for log_file in log_files:
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                parsed = parse_log_line(line.strip())

                if parsed:
                    parsed["hash"] = generate_log_id(parsed)
                    insert_log(conn, parsed)
                else:
                    insert_error(conn, line.strip(), "Malformed log")

def main():
    parser = argparse.ArgumentParser(
        description="Apache Log ETL Pipeline"
    )