from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register


@register(
    task="validation-test",
    dataset="demand.egon_demandregio_hh",
    rule_id="adhoc_NOT_NULL_NAN",
    kind="formal",
    column="demand",
)
class NotNullAndNotNaN(SqlRule):
    def sql(self, ctx):
        col = self.params.get("column", None)
        where = f"WHERE ({col} IS NULL OR {col} <> {col})"
        return f"SELECT COUNT(*) AS n_bad FROM {self.dataset} {where}"

    def postprocess(self, row, ctx):
        n_bad = int(row.get("n_bad") or 0)
        ok = n_bad == 0
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=ok,
            message=f"{n_bad} offending rows (NULL or NaN)",
            severity=Severity.WARNING,
            schema=self.schema,
            table=self.table,
            column=self.params.get("column"),
        )
