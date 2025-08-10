from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="build_lp", dataset="public.load_profiles", rule_id="LP_NOT_NULL_NAN",
          kind="formal", column="value", scenario_col=None)
class NotNullAndNotNaN(SqlRule):
    def sql(self, ctx):
        col = self.params.get("column", "value")
        scenario_col = self.params.get("scenario_col")
        where = f"WHERE ({col} IS NULL OR {col} <> {col})"
        if ctx.scenario and scenario_col:
            where += f" AND {scenario_col} = :scenario"
        return f"SELECT COUNT(*) AS n_bad FROM {self.dataset} {where}"

    def postprocess(self, row, ctx):
        n_bad = int(row.get("n_bad") or 0)
        ok = (n_bad == 0)
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, message=f"{n_bad} offending rows (NULL or NaN)",
            severity=Severity.WARNING, schema=self.schema, table=self.table,
            column=self.params.get("column")
        )

@register(task="build_lp", dataset="public.load_profiles", rule_id="LP_RANGE",
          kind="formal", column="value", min_val=0.0, max_val=1.2, scenario_col=None)
class Range(SqlRule):
    def sql(self, ctx):
        col = self.params.get("column", "value")
        mn = float(self.params.get("min_val", 0.0))
        mx = float(self.params.get("max_val", 1.2))
        scenario_col = self.params.get("scenario_col")
        where = f"WHERE ({col} < {mn} OR {col} > {mx})"
        if ctx.scenario and scenario_col:
            where += f" AND {scenario_col} = :scenario"
        return f"SELECT COUNT(*) AS n_bad FROM {self.dataset} {where}"

    def postprocess(self, row, ctx):
        n_bad = int(row.get("n_bad") or 0)
        ok = (n_bad == 0)
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, message=f"{n_bad} rows outside range",
            severity=Severity.WARNING, schema=self.schema, table=self.table, column=self.params.get("column")
        )

@register(task="energy_balance", dataset="public.balance", rule_id="BAL_DIFF",
          kind="formal", tolerance=0.0)
class AbsDiffWithinTolerance(SqlRule):
    def sql(self, ctx):
        # Example: difference between two aggregates should be ~0
        return "SELECT (SUM(production) - SUM(consumption)) AS diff FROM public.balance"

    def postprocess(self, row, ctx):
        diff = float(row.get("diff") or 0.0)
        tol = float(self.params.get("tolerance", 0.0))
        ok = abs(diff) <= tol
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=diff, expected=0.0,
            message=f"|diff| <= {tol}", severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )

@register(task="topology", dataset="public.lines", rule_id="SRID_UNIQUE_NONZERO",
          kind="formal", geom="geom")
class SRIDUniqueNonZero(SqlRule):
    def sql(self, ctx):
        geom = self.params.get("geom", "geom")
        return f"""
        SELECT COUNT(DISTINCT ST_SRID({geom})) AS srids,
               SUM(CASE WHEN ST_SRID({geom}) = 0 THEN 1 ELSE 0 END) AS srid_zero
        FROM {self.dataset}
        """

    def postprocess(self, row, ctx):
        srids = int(row.get("srids") or 0)
        srid_zero = int(row.get("srid_zero") or 0)
        ok = (srids == 1) and (srid_zero == 0)
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=srids, expected=1.0,
            message="Exactly one SRID and none equals 0",
            severity=Severity.WARNING, schema=self.schema, table=self.table, column=self.params.get("geom", "geom")
        )
