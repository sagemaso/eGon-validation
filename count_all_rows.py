#!/usr/bin/env python3
"""
Count all rows in all tables of the database.
This script will connect to your database and provide a comprehensive row count reporter.
"""

import sys
from egon_validation.config import get_env, ENV_DB_URL, build_db_url
from egon_validation.db import make_engine, fetch_all


def get_all_tables(engine):
    """Get all user tables from the database (excluding system schemas)."""
    query = """
    SELECT schemaname, tablename
    FROM pg_tables
    WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
    ORDER BY schemaname, tablename;
    """
    return fetch_all(engine, query)


def count_table_rows(engine, schema, table):
    """Count rows in a specific table."""
    try:
        # Use quoted identifiers to handle special characters in names
        query = f'SELECT COUNT(*) as row_count FROM "{schema}"."{table}";'
        result = fetch_all(engine, query)
        return result[0]['row_count'] if result else 0
    except Exception as e:
        print(f"Error counting rows in {schema}.{table}: {e}")
        return -1  # Indicate error


def main():
    # Get database URL
    db_url = get_env(ENV_DB_URL) or build_db_url()

    if not db_url:
        print("❌ No database URL configured!")
        print("Set EGON_DB_URL environment variable or configure .env file with DB_* variables")
        sys.exit(1)

    try:
        # Connect to database
        engine = make_engine(db_url)
        print(f"✅ Connected to database")

        # Get all tables
        tables = get_all_tables(engine)

        if not tables:
            print("No user tables found in the database.")
            return

        print(f"\nFound {len(tables)} tables. Counting rows...\n")

        total_rows = 0
        schema_totals = {}

        # Count rows in each table
        for table_info in tables:
            schema = table_info['schemaname']
            table = table_info['tablename']

            row_count = count_table_rows(engine, schema, table)

            if row_count >= 0:
                print(f"{schema}.{table:<40} {row_count:>12,} rows")
                total_rows += row_count
                schema_totals[schema] = schema_totals.get(schema, 0) + row_count
            else:
                print(f"{schema}.{table:<40} {'ERROR':>12}")

        # Print summary
        print("\n" + "="*60)
        print("SUMMARY BY SCHEMA:")
        print("="*60)

        for schema, count in sorted(schema_totals.items()):
            print(f"{schema:<40} {count:>12,} rows")

        print("-"*60)
        print(f"{'TOTAL':<40} {total_rows:>12,} rows")
        print(f"{'TABLES':<40} {len(tables):>12,} tables")

    except Exception as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()