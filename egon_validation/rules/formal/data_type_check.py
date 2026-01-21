from egon_validation.rules.base import (
    SqlRule,
    POSTGRES_TYPE_MAPPINGS,
    Severity,
)
from egon_validation.rules.registry import register


@register(
    task="validation-test",
    table="demand.egon_demandregio_hh",
    rule_id="DATA_TYPE_CHECK",
    column_types={"year": "integer", "scenario": "text", "demand": "numeric"},
)
class DataTypeValidation(SqlRule):
    """Validates data types for one or more columns in a table.

    Args:
        rule_id: Unique identifier
        task: Task identifier
        table: Full table name including schema
        column_types: Dict mapping column names to expected types (passed in params)

    Example (single column):
        >>> validation = DataTypeValidation(
        ...     rule_id="YEAR_TYPE_CHECK",
        ...     task="validation-test",
        ...     table="demand.egon_demandregio_hh",
        ...     column_types={"year": "integer"}
        ... )

    Example (multiple columns):
        >>> validation = DataTypeValidation(
        ...     rule_id="HH_TYPES_CHECK",
        ...     task="validation-test",
        ...     table="demand.egon_demandregio_hh",
        ...     column_types={"year": "integer", "scenario": "text"}
        ... )
    """

    def sql(self, ctx):
        # Modify the query to aggregate all results into a single row with JSON
        column_types = self.params.get("column_types", {})
        columns = list(column_types.keys())
        schema, table = self.get_schema_and_table()
        columns_list = "', '".join(columns)

        return f"""
        SELECT
            json_agg(
                json_build_object(
                    'column_name', column_name,
                    'data_type', data_type,
                    'udt_name', udt_name
                )
            ) as columns_info
        FROM information_schema.columns
        WHERE
            table_schema = '{schema}' AND
            table_name = '{table}' AND
            column_name IN ('{columns_list}')
        """

    def postprocess(self, row, ctx):
        columns_info_json = row.get("columns_info")
        if not columns_info_json:
            return self.error_result("No column information found")

        columns_info = self.parse_json_result(columns_info_json)
        if not columns_info:
            return self.error_result("No column information found")

        column_types = self.params.get("column_types", {})

        problems = []
        found_columns = set()

        for col_info in columns_info:
            column_name = col_info.get("column_name")
            actual_type = (col_info.get("data_type") or "").lower()
            udt_name = (col_info.get("udt_name") or "").lower()
            found_columns.add(column_name)

            if column_name in column_types:
                expected_type = column_types[column_name].lower()
                expected_types = POSTGRES_TYPE_MAPPINGS.get(
                    expected_type, [expected_type]
                )

                if actual_type not in expected_types and udt_name not in expected_types:
                    problems.append(
                        f"{column_name}: got '{actual_type}' (udt: '{udt_name}'), expected {expected_type}"
                    )

        # Check for missing columns
        missing_columns = set(column_types.keys()) - found_columns
        for missing in missing_columns:
            problems.append(f"{missing}: column not found")

        ok = len(problems) == 0
        message = "All column types valid" if ok else "; ".join(problems)

        return self.create_result(
            success=ok,
            observed=len(problems),
            expected=0,
            message=message,
            severity=Severity.ERROR if not ok else Severity.INFO,
        )
