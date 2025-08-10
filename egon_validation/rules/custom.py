from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="build_lp", dataset="public.load_profiles", rule_id="LP_CUSTOM_Smoothing",
          kind="custom")
class ExampleCustomCheck(SqlRule):
    def sql(self, ctx):
        # Placeholder example: all rows should have smoothing flag = true in scenario, if provided
        where = "WHERE TRUE"
        if ctx.scenario:
            where += " AND scenario = :scenario"
        return f"SELECT COUNT(*) AS n_bad FROM {self.dataset} {where} AND (smoothing_flag IS NOT TRUE)"

    def postprocess(self, row, ctx):
        n_bad = int(row.get("n_bad") or 0)
        ok = (n_bad == 0)
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, message=f"{n_bad} rows violate custom smoothing rule",
            severity=Severity.WARNING, schema=self.schema, table=self.table
        )
