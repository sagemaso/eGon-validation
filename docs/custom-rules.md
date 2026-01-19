# Custom Rules

## SQL Rule

Push validation logic to the database:

```python
from egon_validation.rules.base import SqlRule
from egon_validation.rules.registry import register

@register(
    task="my_task",
    table="schema.my_table",
    rule_id="POSITIVE_VALUES",
    kind="custom",
    column="amount"
)
class PositiveValuesCheck(SqlRule):
    def sql(self, ctx):
        col = self.params["column"]
        return f"""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN {col} < 0 THEN 1 END) as invalid
            FROM {self.table}
        """

    def postprocess(self, row, ctx):
        return self.create_result(
            success=row["invalid"] == 0,
            observed=row["invalid"],
            expected=0
        )
```

## DataFrame Rule

For complex Python-based validation:

```python
from egon_validation.rules.base import DataFrameRule

@register(
    task="my_task",
    table="schema.my_table",
    rule_id="COMPLEX_CHECK",
    kind="custom"
)
class ComplexCheck(DataFrameRule):
    def sql(self, ctx):
        return f"SELECT * FROM {self.table}"

    def validate(self, df, ctx):
        # Custom pandas logic
        invalid = df[df["value"] < df["threshold"]].shape[0]
        return self.create_result(
            success=invalid == 0,
            observed=invalid,
            expected=0
        )
```

## File Location

Place custom rules in `egon_validation/rules/custom/`. They are auto-discovered on import.