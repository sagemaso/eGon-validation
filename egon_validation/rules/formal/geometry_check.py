from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(task="adhoc", dataset="supply.egon_power_plants_wind", rule_id="WIND_PLANTS_IN_GERMANY",
          kind="formal", geometry_column="geom", reference_dataset="boundaries.vg250_sta", 
          reference_geometry="geometry", reference_filter="nuts = 'DE' AND gf = 4",
          filter_condition="site_type = 'Windkraft an Land'")
class GeometryContainmentValidation(SqlRule):
    """Validates that point geometries are contained within reference polygon geometries."""
    
    def sql(self, ctx):
        geom_col = self.params.get("geometry_column", "geom")
        ref_dataset = self.params.get("reference_dataset")
        ref_geom_col = self.params.get("reference_geometry", "geometry")
        ref_filter = self.params.get("reference_filter", "TRUE")
        filter_condition = self.params.get("filter_condition", "TRUE")
        scenario_col = self.params.get("scenario_col")
        
        base_query = f"""
        SELECT 
            COUNT(*) as total_points,
            COUNT(CASE WHEN NOT ST_Disjoint(ST_Transform(points.{geom_col}, 3035), ST_Transform(reference.{ref_geom_col}, 3035)) THEN 1 END) as points_inside,
            COUNT(CASE WHEN ST_Disjoint(ST_Transform(points.{geom_col}, 3035), ST_Transform(reference.{ref_geom_col}, 3035)) THEN 1 END) as points_outside
        FROM 
            {ref_dataset} AS reference,
            {self.dataset} AS points
        WHERE 
            reference.{ref_filter} AND
            points.{filter_condition}
        """
        
        if ctx.scenario and scenario_col:
            base_query += f" AND points.{scenario_col} = :scenario"
            
        return base_query

    def postprocess(self, row, ctx):
        total_points = int(row.get("total_points") or 0)
        points_inside = int(row.get("points_inside") or 0)  
        points_outside = int(row.get("points_outside") or 0)
        
        ok = (points_outside == 0)
        
        if ok:
            message = f"All {total_points} points are within reference boundary"
        else:
            message = f"{points_outside} points are outside reference boundary ({points_inside} inside)"
        
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=float(points_outside), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table, 
            column=self.params.get("geometry_column")
        )


@register(task="adhoc", dataset="supply.egon_power_plants_pv", rule_id="PV_PLANTS_SRID_GEOMETRY",
          kind="formal", geometry_column="geom")
class GeometrySRIDValidation(SqlRule):
    """Validates SRID consistency and non-zero SRID for PostGIS geometry columns."""
    
    def sql(self, ctx):
        geom_col = self.params.get("geometry_column", "geom")
        scenario_col = self.params.get("scenario_col")
        
        base_query = f"""
        SELECT
            COUNT(DISTINCT ST_SRID({geom_col})) AS srid_count,
            SUM(CASE WHEN ST_SRID({geom_col}) = 0 THEN 1 ELSE 0 END) AS srid_is_zero_count,
            COUNT(*) AS total_geometries,
            array_agg(DISTINCT ST_SRID({geom_col})) AS found_srids
        FROM {self.dataset}
        """
        
        where_conditions = []
        if ctx.scenario and scenario_col:
            where_conditions.append(f"{scenario_col} = :scenario")
            
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
            
        return base_query

    def postprocess(self, row, ctx):
        srid_count = int(row.get("srid_count") or 0)
        srid_is_zero_count = int(row.get("srid_is_zero_count") or 0)
        total_geometries = int(row.get("total_geometries") or 0)
        found_srids = row.get("found_srids", [])
        
        ok = (srid_count == 1) and (srid_is_zero_count == 0)
        
        if ok:
            message = f"All {total_geometries} geometries have consistent non-zero SRID: {found_srids[0] if found_srids else 'None'}"
        else:
            problems = []
            if srid_count != 1:
                problems.append(f"Multiple SRIDs found: {found_srids}")
            if srid_is_zero_count > 0:
                problems.append(f"{srid_is_zero_count} geometries with SRID=0")
            message = "; ".join(problems)
        
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=float(srid_count if srid_count != 1 else srid_is_zero_count), 
            expected=1.0 if srid_count != 1 else 0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table,
            column=self.params.get("geometry_column")
        )