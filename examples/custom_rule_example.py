#!/usr/bin/env python3
"""Example of creating custom validation rules.

This example shows how to create both SQL-based and Python-based
custom validation rules for domain-specific data quality checks.
"""

from egon_validation.rules.base import SqlRule, Rule, RuleResult, Severity
from egon_validation.rules.registry import register
from egon_validation import db


# Example 1: SQL-based custom rule
@register(
    task="data_quality",
    dataset="public.example_table",
    rule_id="ENERGY_BALANCE_CHECK",
    kind="custom",
    tolerance=0.05,  # 5% tolerance
)
class EnergyBalanceRule(SqlRule):
    """Check that energy production equals consumption within tolerance."""

    def sql(self, ctx):
        tolerance = self.params.get("tolerance", 0.0)
        return f"""
        SELECT 
            SUM(production) as total_production,
            SUM(consumption) as total_consumption,
            ABS(SUM(production) - SUM(consumption)) as imbalance
        FROM {self.dataset}
        """

    def postprocess(self, row, ctx):
        production = float(row.get("total_production", 0))
        consumption = float(row.get("total_consumption", 0))
        imbalance = float(row.get("imbalance", 0))

        tolerance = self.params.get("tolerance", 0.0)
        max_allowed_imbalance = production * tolerance

        success = imbalance <= max_allowed_imbalance

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=success,
            observed=imbalance,
            expected=f"≤{max_allowed_imbalance:.2f}",
            message=f"Energy imbalance: {imbalance:.2f} (tolerance: {tolerance*100:.1f}%)",
            severity=Severity.ERROR if not success else Severity.INFO,
            schema=self.schema,
            table=self.table,
        )


# Example 2: Python-based custom rule
@register(
    task="data_integrity",
    dataset="public.coordinates",
    rule_id="COORDINATE_BOUNDS_CHECK",
    kind="custom",
)
class CoordinateBoundsRule(Rule):
    """Check that coordinates are within Germany's bounding box."""

    def evaluate(self, engine, ctx):
        # Germany approximate bounds
        MIN_LAT, MAX_LAT = 47.0, 55.0
        MIN_LON, MAX_LON = 5.5, 15.5

        try:
            # Count invalid coordinates
            query = f"""
            SELECT COUNT(*) as invalid_count
            FROM {self.dataset}
            WHERE lat < {MIN_LAT} OR lat > {MAX_LAT} 
               OR lon < {MIN_LON} OR lon > {MAX_LON}
            """

            result = db.fetch_one(engine, query)
            invalid_count = int(result.get("invalid_count", 0))

            # Get total count
            total_query = f"SELECT COUNT(*) as total FROM {self.dataset}"
            total_result = db.fetch_one(engine, total_query)
            total_count = int(total_result.get("total", 0))

            success = invalid_count == 0

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                dataset=self.dataset,
                success=success,
                observed=invalid_count,
                expected=0,
                message=f"{invalid_count}/{total_count} coordinates outside Germany bounds",
                severity=Severity.WARNING if not success else Severity.INFO,
                schema=self.schema,
                table=self.table,
            )

        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                dataset=self.dataset,
                success=False,
                message=f"Rule execution failed: {str(e)}",
                severity=Severity.ERROR,
            )


# Example 3: Parameterized rule using register_map
from egon_validation.rules.registry import register_map
from egon_validation.rules.formal.null_check import NotNullCheck

# Register null checks for multiple critical columns
register_map(
    task="data_completeness",
    rule_cls=NotNullCheck,
    rule_id="CRITICAL_NULLS",
    kind="formal",
    datasets_params={
        "public.generators": {"column": "p_nom"},
        "public.loads": {"column": "annual_consumption"},
        "public.buses": {"column": "v_nom"},
    },
)


def main():
    """Demonstrate custom rule registration."""
    from egon_validation.rules.registry import list_registered

    print("Registered custom rules:")
    for rule in list_registered():
        if rule["kind"] == "custom":
            print(f"  - {rule['rule_id']}: {rule['dataset']}")


if __name__ == "__main__":
    main()
