from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

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