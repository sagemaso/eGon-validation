from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register


@register(
    task="validation-test",
    dataset="supply.egon_power_plants_wind",
    rule_id="WIND_PLANTS_IN_GERMANY",
    kind="formal",
    geometry_column="geom",
    reference_dataset="boundaries.vg250_sta",
    reference_geometry="geometry",
    reference_filter="nuts = 'DE' AND gf = 4",
    filter_condition="site_type = 'Windkraft an Land'",
)
class GeometryContainmentValidation(SqlRule):
    """Validates that point geometries are contained within reference polygon geometries."""

    def sql(self, ctx):
        geom_col = self.params.get("geometry_column", "geom")
        ref_dataset = self.params.get("reference_dataset")
        ref_geom_col = self.params.get("reference_geometry", "geometry")
        ref_filter = self.params.get("reference_filter", "TRUE")
        filter_condition = self.params.get("filter_condition", "TRUE")

        base_query = f"""
        WITH reference_geom AS (
            SELECT ST_Union(ST_Transform({ref_geom_col}, 3035)) as unified_geom
            FROM {ref_dataset}
            WHERE {ref_filter}
        )
        SELECT 
            COUNT(*) as total_points,
            COUNT(CASE WHEN ST_Contains(reference_geom.unified_geom, ST_Transform(points.{geom_col}, 3035)) THEN 1 END) as points_inside,
            COUNT(CASE WHEN NOT ST_Contains(reference_geom.unified_geom, ST_Transform(points.{geom_col}, 3035)) THEN 1 END) as points_outside
        FROM 
            reference_geom,
            {self.dataset} AS points
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

            # Add debugging information for wind plants specifically
            if self.rule_id == "WIND_PLANTS_IN_GERMANY" and points_outside > 0:
                message += f" | To get coordinates: SELECT * FROM supply.egon_power_plants_wind WHERE site_type = 'Windkraft an Land'"
                message += f" | AND NOT ST_Contains((SELECT ST_Union(ST_Transform(geometry, 3035)) FROM boundaries.vg250_sta WHERE nuts = 'DE' AND gf = 4), ST_Transform(geom, 3035))"

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=ok,
            observed=float(points_outside),
            expected=0.0,
            message=message,
            severity=Severity.WARNING,
            schema=self.schema,
            table=self.table,
            column=self.params.get("geometry_column"),
        )