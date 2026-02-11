from egon_validation.rules.base import SqlRule, Severity
from egon_validation.rules.registry import register
from egon_validation.config import DEFAULT_SRID


@register(
    task="validation-test",
    table="supply.egon_power_plants_pv",
    rule_id="SRID_UNIQUE_NONZERO",
    geom="geom",
)
class SRIDUniqueNonZero(SqlRule):
    def get_query(self, ctx):
        geom = self.params.get("geom", "geom")
        return f"""
        SELECT COUNT(DISTINCT ST_SRID({geom})) AS srids,
               SUM(CASE WHEN ST_SRID({geom}) = 0 THEN 1 ELSE 0 END) AS srid_zero
        FROM {self.table}
        """

    def postprocess(self, row, ctx):
        srids = int(row.get("srids") or 0)
        srid_zero = int(row.get("srid_zero") or 0)
        ok = (srids == 1) and (srid_zero == 0)
        return self.create_result(
            success=ok,
            observed=srids,
            expected=1.0,
            message="Exactly one SRID and none equals 0",
            column=self.params.get("geom", "geom"),
        )


# @register(task="validation-test", table="supply.egon_power_plants_pv", rule_id="PV_PLANTS_SRID_VALIDATION",
# geom="geom", expected_srid=3035)
class SRIDSpecificValidation(SqlRule):
    """Validates that geometry column has a specific expected SRID."""

    def get_query(self, ctx):
        geom = self.params.get("geom", "geom")
        expected_srid = self.params.get("expected_srid", DEFAULT_SRID)

        base_query = f"""
        SELECT
            COUNT(*) AS total_geometries,
            COUNT(DISTINCT ST_SRID({geom})) AS unique_srids,
            SUM(CASE WHEN ST_SRID({geom}) = {expected_srid} THEN 1 ELSE 0 END) AS correct_srid_count,
            SUM(CASE WHEN ST_SRID({geom}) = 0 THEN 1 ELSE 0 END) AS zero_srid_count,
            array_agg(DISTINCT ST_SRID({geom})) AS found_srids
        FROM {self.table}
        """

        return base_query

    def postprocess(self, row, ctx):
        total_geometries = int(row.get("total_geometries") or 0)
        unique_srids = int(row.get("unique_srids") or 0)
        correct_srid_count = int(row.get("correct_srid_count") or 0)
        zero_srid_count = int(row.get("zero_srid_count") or 0)
        found_srids = row.get("found_srids", [])
        expected_srid = int(self.params.get("expected_srid", 4326))

        ok = (
            (unique_srids == 1)
            and (correct_srid_count == total_geometries)
            and (zero_srid_count == 0)
        )

        if ok:
            message = (
                f"All {total_geometries} geometries have correct SRID {expected_srid}"
            )
        else:
            problems = []
            if unique_srids != 1:
                problems.append(f"Multiple SRIDs found: {found_srids}")
            if correct_srid_count != total_geometries:
                problems.append(
                    f"Only {correct_srid_count}/{total_geometries} have expected SRID {expected_srid}"
                )
            if zero_srid_count > 0:
                problems.append(f"{zero_srid_count} geometries with SRID=0")
            message = "; ".join(problems)

        return self.create_result(
            success=ok,
            observed=correct_srid_count,
            expected=total_geometries,
            message=message,
            column=self.params.get("geom", "geom"),
            severity=Severity.ERROR if not ok else Severity.INFO,
        )
