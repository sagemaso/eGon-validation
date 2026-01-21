from egon_validation.rules.base import Rule, SqlRule, Severity
from egon_validation.rules.registry import register


@register(
    task="validation-test",
    table="demand.egon_demandregio_hh",
    rule_id="adhoc_NOT_NULL_NAN",
    columns=["demand", "year"],
)
class NotNullAndNotNaNValidation(SqlRule):
    """Validates that one or more columns contain no NULL or NaN values.

    Args:
        rule_id: Unique identifier
        task: Task identifier
        table: Full table name including schema
        columns: List of column names to check (passed in params). Can be a single column or multiple.

    Example (single column):
        >>> validation = NotNullAndNotNaN(
        ...     rule_id="TS_SCENARIO_NOT_NULL",
        ...     task="validation-test",
        ...     table="facts.timeseries",
        ...     columns=["scenario_id"]
        ... )

    Example (multiple columns):
        >>> validation = NotNullAndNotNaN(
        ...     rule_id="HH_COLUMNS_NOT_NULL",
        ...     task="validation-test",
        ...     table="demand.egon_demandregio_hh",
        ...     columns=["year", "demand", "scenario"]
        ... )
    """

    def sql(self, ctx):
        columns = self.params.get("columns", [])
        if not columns:
            return "SELECT NULL as columns_info"

        schema, table = self.get_schema_and_table()

        # Build JSON aggregation for each column's NULL/NaN count
        column_checks = []
        for col in columns:
            column_checks.append(
                f"""(json_build_object(
                    'column_name', '{col}',
                    'null_nan_count', (
                        SELECT COUNT(*)
                        FROM {self.table}
                        WHERE {col} IS NULL OR {col} <> {col}
                    )
                ))"""
            )

        checks_sql = ",\n                ".join(column_checks)

        return f"""
        SELECT json_agg(column_info) as columns_info
        FROM (
            SELECT * FROM (VALUES
                {checks_sql}
            ) AS t(column_info)
        ) AS subquery
        """

    def postprocess(self, row, ctx):
        columns_info_json = row.get("columns_info")
        if not columns_info_json:
            return self.error_result("No column information found")

        columns_info = self.parse_json_result(columns_info_json)
        if not columns_info:
            return self.error_result("No column information found")

        problems = []
        total_bad = 0

        for col_info in columns_info:
            column_name = col_info.get("column_name")
            null_nan_count = int(col_info.get("null_nan_count", 0))

            if null_nan_count > 0:
                problems.append(f"{column_name}: {null_nan_count} NULL/NaN values")
                total_bad += null_nan_count

        ok = len(problems) == 0

        # Build message based on number of columns
        columns = self.params.get("columns", [])
        if len(columns) == 1:
            message = (
                f"Column '{columns[0]}' has no NULL/NaN values" if ok else problems[0]
            )
        else:
            message = (
                f"All {len(columns)} specified columns have no NULL/NaN values"
                if ok
                else "; ".join(problems)
            )

        return self.create_result(
            success=ok,
            observed=total_bad,
            expected=0,
            message=message,
            severity=Severity.ERROR if not ok else Severity.INFO,
        )


@register(
    task="validation-test",
    table="demand.egon_demandregio_hh",
    rule_id="WHOLE_TABLE_NOT_NULL_NAN",
)
class WholeTableNotNullAndNotNaNValidation(Rule):
    """Validates that all columns in a table contain no NULL or NaN values.

    This rule automatically discovers all columns in the table and checks each one.
    Uses the evaluate() method to query columns and check each one in a loop.

    Args:
        rule_id: Unique identifier
        task: Task identifier
        table: Full table name including schema

    Example:
        >>> validation = WholeTableNotNullAndNotNaN(
        ...     rule_id="HH_TABLE_NOT_NULL",
        ...     task="validation-test",
        ...     table="demand.egon_demandregio_hh"
        ... )
    """

    def evaluate(self, engine, ctx):
        """Execute rule by querying all columns and checking each one."""
        from egon_validation import db

        schema, table = self.get_schema_and_table()

        # Step 1: Get all columns from information_schema
        columns_query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
            ORDER BY ordinal_position
        """

        try:
            columns_result = db.fetch_all(engine, columns_query)
        except Exception as e:
            return self.error_result(
                message=f"Failed to fetch column information: {str(e)}"
            )

        if not columns_result:
            return self.error_result(message="No columns found in table")

        # Step 2: Check each column for NULL/NaN values
        problems = []
        total_bad = 0
        total_columns = len(columns_result)
        columns_with_issues = 0

        for col_info in columns_result:
            col_name = col_info.get("column_name")
            col_type = col_info.get("data_type")

            # Build WHERE condition based on data type
            if col_type in ("double precision", "real", "numeric"):
                # Check for NULL or NaN (NaN != NaN)
                where_condition = (
                    f'"{col_name}" IS NULL OR "{col_name}" <> "{col_name}"'
                )
            else:
                # Just check for NULL
                where_condition = f'"{col_name}" IS NULL'

            # Query to count NULL/NaN values for this column
            count_query = f"""
                SELECT COUNT(*) as null_nan_count
                FROM {self.table}
                WHERE {where_condition}
            """

            try:
                result = db.fetch_one(engine, count_query)
                null_nan_count = int(result.get("null_nan_count", 0))

                if null_nan_count > 0:
                    problems.append(f"{col_name}: {null_nan_count} NULL/NaN values")
                    total_bad += null_nan_count
                    columns_with_issues += 1

            except Exception as e:
                problems.append(f"{col_name}: error checking ({str(e)})")
                columns_with_issues += 1

        # Step 3: Build result
        ok = len(problems) == 0
        message = (
            f"All {total_columns} columns have no NULL/NaN values"
            if ok
            else f"{columns_with_issues}/{total_columns} columns have issues: {'; '.join(problems)}"
        )

        return self.create_result(
            success=ok,
            observed=total_bad,
            expected=0,
            message=message,
            severity=Severity.ERROR if not ok else Severity.INFO,
        )
