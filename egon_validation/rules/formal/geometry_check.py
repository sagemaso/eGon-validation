from egon_validation.rules.base import SqlRule, Severity
from egon_validation.rules.registry import register


@register(
    task="validation-test",
    table="supply.egon_power_plants_wind",
    rule_id="WIND_PLANTS_IN_GERMANY",
    geom="geom",
    ref_table="boundaries.vg250_sta",
    ref_geom="geometry",
    ref_filter="nuts = 'DE' AND gf = 4",
    filter_condition="site_type = 'Windkraft an Land'",
)
class GeometryContainmentValidation(SqlRule):
    """Validates that point geometries are contained within reference polygon geometries."""

    def sql(self, ctx):
        geom_col = self.params.get("geom", "geom")
        ref_table = self.params.get("ref_table")
        ref_geom_col = self.params.get("ref_geom", "geometry")
        ref_filter = self.params.get("ref_filter", "TRUE")
        filter_condition = self.params.get("filter_condition", "TRUE")

        base_query = f"""
        WITH reference_geom AS (
            SELECT ST_Union(ST_Transform({ref_geom_col}, 3035)) as unified_geom
            FROM {ref_table}
            WHERE {ref_filter}
        )
        SELECT
            COUNT(*) as total_points,
            COUNT(CASE WHEN ST_Contains(reference_geom.unified_geom, ST_Transform(points.{geom_col}, 3035)) THEN 1 END) as points_inside,
            COUNT(CASE WHEN NOT ST_Contains(reference_geom.unified_geom, ST_Transform(points.{geom_col}, 3035)) THEN 1 END) as points_outside
        FROM
            reference_geom,
            {self.table} AS points
        WHERE
            points.{filter_condition}
        """

        return base_query

    def postprocess(self, row, ctx):
        total_points = int(row.get("total_points") or 0)
        points_inside = int(row.get("points_inside") or 0)
        points_outside = int(row.get("points_outside") or 0)

        ok = points_outside == 0
        filter_condition = self.params.get("filter_condition", "TRUE")
        ref_filter = self.params.get("reference_filter", "TRUE")

        if ok:
            message = f"All {total_points} points are within reference boundary (filter: {filter_condition})"
        else:
            message = f"{points_outside} points are outside reference boundary ({points_inside} inside)"
            message += f" [Filter: {filter_condition}, Ref: {ref_filter}]"

        return self.create_result(
            success=ok,
            observed=points_outside,
            expected=0,
            message=message,
            column=self.params.get("geom", "geom"),
            severity=Severity.ERROR if not ok else Severity.INFO,
        )
