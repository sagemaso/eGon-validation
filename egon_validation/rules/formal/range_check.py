from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

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