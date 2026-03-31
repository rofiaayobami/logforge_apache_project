import sqlite3
import json
import csv
from pathlib import Path

DB_PATH = Path("db/logs.db")

def top_endpoints(conn, limit=10):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT path, COUNT(*) as count
        FROM logs
        GROUP BY path
        ORDER BY count DESC
        LIMIT ?
    """, (limit,))
    return cursor.fetchall()

def status_distribution(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT status, COUNT(*) as count
        FROM logs
        GROUP BY status
        ORDER BY count DESC
    """)
    return cursor.fetchall()

def top_ips(conn, limit=10):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ip, COUNT(*) as count
        FROM logs
        GROUP BY ip
        ORDER BY count DESC
        LIMIT ?
    """, (limit,))
    return cursor.fetchall()

def save_summary(data, output_file, format="json"):
    if format == "json":
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
    elif format == "csv":
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            
            for section, rows in data.items():
                writer.writerow([section])  # Section header
                if len(rows) == 0:
                    writer.writerow(["No data"])
                    continue

                # Write column headers
                writer.writerow(rows[0].keys())
                # Write row data
                for row in rows:
                    writer.writerow(row.values())
                writer.writerow([])  # Blank line between sections


def generate_report(db_path=DB_PATH, output_format="json"):
    conn = sqlite3.connect(db_path)

    report = {
        "top_endpoints": [{"path": path, "count": count} for path, count in top_endpoints(conn)],
        "status_distribution": [{"status": status, "count": count} for status, count in status_distribution(conn)],
        "top_ips": [{"ip": ip, "count": count} for ip, count in top_ips(conn)]
    }

    save_summary(report, f"summary.{output_format}", format=output_format)
    conn.close()
    print(f" Summary saved as summary.{output_format}")

import argparse

def main():
    parser = argparse.ArgumentParser(description="Generate summaries for Apache logs")
    parser.add_argument("--output-format", choices=["json", "csv"], default="json")
    args = parser.parse_args()

    generate_report(output_format=args.output_format)

if __name__ == "__main__":
    main()

