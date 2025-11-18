from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register


@register(
    task="validation-test",
    dataset="demand.egon_demandregio_hh",
    rule_id="COLUMN_DATA_TYPE_CHECK",
    kind="formal",
    column="year",
    expected_type="integer",
)
class DataTypeValidation(SqlRule):
    """Validates that a column has the expected PostgreSQL data type.

    Args:
        table: Full table name including schema
        column: Column name to check
        expected_type: Expected PostgreSQL data type
        rule_id: Unique identifier
        kind: Validation kind (default: "formal")

    Example:
        >>> validation = DataTypeValidation(
        ...     table="demand.egon_demandregio_hh",
        ...     column="year",
        ...     expected_type="integer",
        ...     rule_id="YEAR_TYPE_CHECK"
        ... )
    """

    def __init__(self, table: str, column: str, expected_type: str,
                 rule_id: str, kind: str = "formal"):
        """Initialize data type validation."""
        super().__init__(
            rule_id=rule_id,
            task="inline",
            dataset=table,
            column=column,
            expected_type=expected_type,
            kind=kind
        )

    def sql(self, ctx):
        column = self.params.get("column", "id")
        expected_type = self.params.get("expected_type", "integer").lower()

        # Split dataset to get schema and table
        if "." in self.dataset:
            schema, table = self.dataset.split(".", 1)
        else:
            schema, table = "public", self.dataset

        return f"""
        SELECT 
            column_name,
            data_type,
            udt_name,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE 
            table_schema = '{schema}' AND
            table_name = '{table}' AND
            column_name = '{column}'
        """

    def postprocess(self, row, ctx):
        if not row:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                dataset=self.dataset,
                success=False,
                message="Column not found",
                severity=Severity.ERROR,
                schema=self.schema,
                table=self.table,
                column=self.params.get("column"),
            )

        column_name = row.get("column_name")
        actual_type = row.get("data_type", "").lower()
        udt_name = row.get("udt_name", "").lower()
        expected_type = self.params.get("expected_type", "integer").lower()

        # PostgreSQL type mapping
        type_mappings = {
            "integer": ["integer", "int4", "int", "bigint", "int8", "smallint", "int2"],
            "text": ["text", "character varying", "varchar", "character", "char"],
            "numeric": [
                "numeric",
                "decimal",
                "real",
                "double precision",
                "float4",
                "float8",
            ],
            "boolean": ["boolean", "bool"],
            "timestamp": [
                "timestamp without time zone",
                "timestamp with time zone",
                "timestamptz",
            ],
            "date": ["date"],
            "uuid": ["uuid"],
            "geometry": ["geometry", "geography"],
            "array": ["array", "_int4", "_text", "_numeric"],
        }

        expected_types = type_mappings.get(expected_type, [expected_type])
        ok = actual_type in expected_types or udt_name in expected_types

        message = f"Column '{column_name}' has type '{actual_type}' (udt: '{udt_name}'), expected: {expected_type}"

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=ok,
            message=message,
            severity=Severity.WARNING,
            schema=self.schema,
            table=self.table,
            column=column_name,
        )


@register(
    task="validation-test",
    dataset="demand.egon_demandregio_hh",
    rule_id="MULTIPLE_COLUMNS_TYPE_CHECK",
    kind="formal",
    column_types={"year": "integer", "scenario": "text", "demand": "numeric"},
)
class MultipleColumnsDataTypeValidation(SqlRule):
    """Validates data types for multiple columns in a table.

    Args:
        table: Full table name including schema
        column_types: Dict mapping column names to expected types
        rule_id: Unique identifier
        kind: Validation kind (default: "formal")

    Example:
        >>> validation = MultipleColumnsDataTypeValidation(
        ...     table="demand.egon_demandregio_hh",
        ...     column_types={"year": "integer", "scenario": "text"},
        ...     rule_id="HH_TYPES_CHECK"
        ... )
    """

    def __init__(self, table: str, column_types: dict, rule_id: str,
                 kind: str = "formal"):
        """Initialize multiple columns data type validation."""
        super().__init__(
            rule_id=rule_id,
            task="inline",
            dataset=table,
            column_types=column_types,
            kind=kind
        )

    def sql(self, ctx):
        # Modify the query to aggregate all results into a single row with JSON
        column_types = self.params.get("column_types", {})
        columns = list(column_types.keys())

        if "." in self.dataset:
            schema, table = self.dataset.split(".", 1)
        else:
            schema, table = "public", self.dataset

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
        import json

        columns_info_json = row.get("columns_info")
        if not columns_info_json:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                dataset=self.dataset,
                success=False,
                message="No column information found",
                severity=Severity.ERROR,
                schema=self.schema,
                table=self.table,
            )

        columns_info = (
            json.loads(columns_info_json)
            if isinstance(columns_info_json, str)
            else columns_info_json
        )
        if not columns_info:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                dataset=self.dataset,
                success=False,
                message="No column information found",
                severity=Severity.ERROR,
                schema=self.schema,
                table=self.table,
            )

        column_types = self.params.get("column_types", {})
        type_mappings = {
            "integer": ["integer", "int4", "int", "bigint", "int8", "smallint", "int2"],
            "text": ["text", "character varying", "varchar", "character", "char"],
            "numeric": [
                "numeric",
                "decimal",
                "real",
                "double precision",
                "float4",
                "float8",
            ],
            "boolean": ["boolean", "bool"],
            "timestamp": [
                "timestamp without time zone",
                "timestamp with time zone",
                "timestamptz",
            ],
            "date": ["date"],
            "uuid": ["uuid"],
            "geometry": ["geometry", "geography"],
            "array": ["array", "_int4", "_text", "_numeric"],
        }

        problems = []
        found_columns = set()

        for col_info in columns_info:
            column_name = col_info.get("column_name")
            actual_type = (col_info.get("data_type") or "").lower()
            udt_name = (col_info.get("udt_name") or "").lower()
            found_columns.add(column_name)

            if column_name in column_types:
                expected_type = column_types[column_name].lower()
                expected_types = type_mappings.get(expected_type, [expected_type])

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

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=ok,
            observed=float(len(problems)),
            expected=0.0,
            message=message,
            severity=Severity.WARNING,
            schema=self.schema,
            table=self.table,
        )
