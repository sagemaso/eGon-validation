from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register, register_map
from egon_validation.config import DEFAULT_SRID

@register(task="adhoc", dataset="supply.egon_power_plants_pv", rule_id="SRID_UNIQUE_NONZERO",
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


#@register(task="adhoc", dataset="supply.egon_power_plants_pv", rule_id="PV_PLANTS_SRID_VALIDATION",
#          kind="formal", geom="geom", expected_srid=3035)
class SRIDSpecificValidation(SqlRule):
    """Validates that geometry column has a specific expected SRID."""
    
    def sql(self, ctx):
        geom = self.params.get("geom", "geom")
        expected_srid = self.params.get("expected_srid", DEFAULT_SRID)
        scenario_col = self.params.get("scenario_col")
        
        base_query = f"""
        SELECT 
            COUNT(*) AS total_geometries,
            COUNT(DISTINCT ST_SRID({geom})) AS unique_srids,
            SUM(CASE WHEN ST_SRID({geom}) = {expected_srid} THEN 1 ELSE 0 END) AS correct_srid_count,
            SUM(CASE WHEN ST_SRID({geom}) = 0 THEN 1 ELSE 0 END) AS zero_srid_count,
            array_agg(DISTINCT ST_SRID({geom})) AS found_srids
        FROM {self.dataset}
        """
        
        if ctx.scenario and scenario_col:
            base_query += f" WHERE {scenario_col} = :scenario"
            
        return base_query

    def postprocess(self, row, ctx):
        total_geometries = int(row.get("total_geometries") or 0)
        unique_srids = int(row.get("unique_srids") or 0)
        correct_srid_count = int(row.get("correct_srid_count") or 0)
        zero_srid_count = int(row.get("zero_srid_count") or 0)
        found_srids = row.get("found_srids", [])
        expected_srid = int(self.params.get("expected_srid", 4326))
        
        ok = (unique_srids == 1) and (correct_srid_count == total_geometries) and (zero_srid_count == 0)
        
        if ok:
            message = f"All {total_geometries} geometries have correct SRID {expected_srid}"
        else:
            problems = []
            if unique_srids != 1:
                problems.append(f"Multiple SRIDs found: {found_srids}")
            if correct_srid_count != total_geometries:
                problems.append(f"Only {correct_srid_count}/{total_geometries} have expected SRID {expected_srid}")
            if zero_srid_count > 0:
                problems.append(f"{zero_srid_count} geometries with SRID=0")
            message = "; ".join(problems)
        
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=float(unique_srids), expected=1.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table, column=self.params.get("geom")
        )


# Register SRID validation for multiple geometry datasets mentioned in CSV
register_map(
    task="adhoc",
    rule_cls=SRIDSpecificValidation,
    rule_id="SPECIAL_SRID_VALIDATION",
    kind="formal",
    datasets_params={
        "supply.egon_power_plants_wind": {
            "geom": "geom", "expected_srid": 4326
        },
        "boundaries.vg250_sta": {
            "geom": "geometry", "expected_srid": 4326
        },
        "grid.egon_mv_grid_district": {
            "geom": "geom", "expected_srid": 3035
        }
    }
)

# Register SRID validation for multiple geometry datasets mentioned in CSV
register_map(
    task="adhoc",
    rule_cls=SRIDUniqueNonZero,
    rule_id="SRID_VALIDATION",
    kind="formal",
    datasets_params={
        "supply.egon_power_plants_wind": {
            "geom": "geom"
        },
        "boundaries.vg250_sta": {
            "geom": "geometry"
        },
        "grid.egon_mv_grid_district": {
            "geom": "geom"
        }
    }
)