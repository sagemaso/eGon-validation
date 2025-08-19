from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register
from egon_validation.config import BALANCE_CHECK_TOLERANCE

@register(task="energy_balance", dataset="public.balance", rule_id="BAL_DIFF",
          kind="formal", tolerance=0.0)
class AbsDiffWithinTolerance(SqlRule):
    def sql(self, ctx):
        # Example: difference between two aggregates should be ~0
        return "SELECT (SUM(production) - SUM(consumption)) AS diff FROM public.balance"

    def postprocess(self, row, ctx):
        diff = float(row.get("diff") or 0.0)
        tol = float(self.params.get("tolerance", BALANCE_CHECK_TOLERANCE))
        ok = abs(diff) <= tol
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=diff, expected=0.0,
            message=f"|diff| <= {tol}", severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )