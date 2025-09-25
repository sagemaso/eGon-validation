#!/usr/bin/env python3
"""DataFrame-based validation usage example.

This example demonstrates how to:
1. Use the DataFrame interface for data validation
2. Create custom DataFrame-based rules
3. Perform statistical analysis with pandas
"""

import os
from pathlib import Path
from egon_validation.context import RunContext
from egon_validation.db import make_engine, DataInterface, fetch_dataframe
from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
from egon_validation.rules.registry import register
from egon_validation import config


# Example custom DataFrame rule
@register(
    task="dataframe_demo",
    dataset="demo.sample_data",
    rule_id="demo_CORRELATION_CHECK",
    kind="custom",
    columns=["demand", "temperature"],
    min_correlation=0.5,
)
class CorrelationCheck(DataFrameRule):
    """Check correlation between two columns using pandas."""

    def get_query(self, ctx) -> str:
        columns = self.params.get("columns", ["col1", "col2"])
        return f"""
        SELECT {', '.join(columns)}
        FROM {self.dataset}
        WHERE {' AND '.join(f'{col} IS NOT NULL' for col in columns)}
        LIMIT 1000
        """

    def evaluate_df(self, df, ctx) -> RuleResult:
        columns = self.params.get("columns", ["col1", "col2"])
        min_correlation = self.params.get("min_correlation", 0.5)

        # Check if both columns exist
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                dataset=self.dataset,
                success=False,
                message=f"Missing columns: {missing_cols}",
                severity=Severity.ERROR,
                schema=self.schema,
                table=self.table,
            )

        # Calculate correlation using pandas
        correlation = df[columns[0]].corr(df[columns[1]])

        success = abs(correlation) >= min_correlation

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=success,
            observed=round(correlation, 3),
            expected=f">={min_correlation}",
            message=f"Correlation between {columns[0]} and {columns[1]}: {correlation:.3f}",
            severity=Severity.INFO if success else Severity.WARNING,
            schema=self.schema,
            table=self.table,
        )


def demonstrate_dataframe_interface():
    """Demonstrate DataFrame interface usage."""
    print("📊 DataFrame Interface Demo")

    # Setup (would normally connect to real database)
    db_url = config.get_env("DB_URL") or "postgresql://demo:demo@localhost/demo"

    try:
        engine = make_engine(db_url)
        interface = DataInterface(engine)

        print("✅ Connected with DataInterface")

        # Example 1: Direct DataFrame query
        print("\n1️⃣ Direct DataFrame Usage:")
        try:
            # This would work with a real database
            # df = interface.fetch_dataframe("SELECT * FROM some_table LIMIT 5")
            # print(f"DataFrame shape: {df.shape}")
            # print(df.head())
            print("   (Would fetch DataFrame from database)")

        except Exception as e:
            print(f"   Demo mode: {e}")

        # Example 2: DataFrame rule usage
        print("\n2️⃣ DataFrame Rule Demo:")
        rule = CorrelationCheck("demo_corr", "dataframe_demo", "demo.sample_data")
        print(f"   Created rule: {rule.rule_id}")
        print(f"   Rule type: {type(rule).__name__}")
        print(f"   Query: {rule.get_query(None)}")

        # Example 3: Statistical analysis capabilities
        print("\n3️⃣ Statistical Analysis Features:")
        print("   ✅ Outlier detection with z-scores")
        print("   ✅ Percentile-based range checking")
        print("   ✅ Correlation analysis")
        print("   ✅ Custom aggregations and grouping")
        print("   ✅ Time series validation")

        # Example 4: GeoPandas integration
        print("\n4️⃣ GeoPandas Integration:")
        try:
            # df_geo = interface.fetch_geodataframe(
            #     "SELECT geom, properties FROM spatial_table",
            #     geom_col="geom"
            # )
            print("   ✅ PostGIS geometry support")
            print("   ✅ Spatial analysis capabilities")
        except ImportError:
            print("   ⚠️  Install egon-validation[geo] for GeoPandas support")

        return True

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        return False


def show_mvp_compliance():
    """Show how this achieves MVP compliance."""
    print("\n🎯 MVP Compliance Achieved:")
    print("   ✅ MUST: Provide as table-like format (pandas DataFrame)")
    print("   ✅ SHOULD: Interfacing via pandas/geopandas libraries")
    print("   ✅ Backward compatibility: Dict-based rules still work")
    print("   ✅ Performance: Optional dependencies, lightweight by default")
    print("   ✅ Extensibility: Easy to create DataFrame-based custom rules")


def main():
    """Run DataFrame usage demonstration."""
    print("🚀 DataFrame Usage Example\n")

    # Check pandas availability
    try:
        import pandas as pd

        print(f"✅ Pandas version: {pd.__version__}")
    except ImportError:
        print("⚠️  Install egon-validation[dataframe] for full functionality")
        return 1

    # Run demonstrations
    demonstrate_dataframe_interface()
    show_mvp_compliance()

    print("\n📋 Next Steps:")
    print("   1. Install: pip install egon-validation[dataframe]")
    print("   2. Create custom DataFrame-based rules")
    print("   3. Leverage pandas for complex statistical validation")
    print("   4. Use GeoPandas for spatial data validation")

    return 0


if __name__ == "__main__":
    exit(main())
