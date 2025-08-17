from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register, register_map

@register(task="adhoc", dataset="grid.egon_etrago_load", rule_id="LOAD_BUS_REFERENTIAL_INTEGRITY",
          kind="formal", foreign_column="bus", reference_dataset="grid.egon_etrago_bus", 
          reference_column="bus_id")
class ReferentialIntegrityValidation(SqlRule):
    """Validates referential integrity between tables without foreign key constraints."""
    
    def sql(self, ctx):
        foreign_col = self.params.get("foreign_column", "id")
        reference_dataset = self.params.get("reference_dataset")
        reference_col = self.params.get("reference_column", "id")
        scenario_col = self.params.get("scenario_col")
        additional_conditions = self.params.get("additional_conditions", "")
        
        base_query = f"""
        SELECT
            count(*) AS total_rows,
            count(child.{foreign_col}) AS non_null_references,
            count(parent.{reference_col}) AS valid_references,
            count(*) - count(parent.{reference_col}) AS orphaned_references
        FROM
            {self.dataset} as child
        LEFT JOIN
            {reference_dataset} as parent
        ON child.{foreign_col} = parent.{reference_col}
        """
        
        where_conditions = []
        
        if additional_conditions:
            where_conditions.append(additional_conditions)
            
        if ctx.scenario and scenario_col:
            where_conditions.append(f"child.{scenario_col} = :scenario")
            
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
            
        return base_query

    def postprocess(self, row, ctx):
        total_rows = int(row.get("total_rows") or 0)
        non_null_references = int(row.get("non_null_references") or 0)
        valid_references = int(row.get("valid_references") or 0)
        orphaned_references = int(row.get("orphaned_references") or 0)
        
        ok = (orphaned_references == 0)
        
        foreign_col = self.params.get("foreign_column", "id")
        reference_dataset = self.params.get("reference_dataset")
        reference_col = self.params.get("reference_column", "id")
        
        if ok:
            message = f"All {non_null_references} references in {foreign_col} have valid matches in {reference_dataset}.{reference_col}"
        else:
            message = f"{orphaned_references} orphaned references found in {foreign_col} (out of {total_rows} total rows)"
        
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=float(orphaned_references), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table, 
            column=foreign_col
        )


# Register multiple referential integrity checks
register_map(
    task="adhoc",
    rule_cls=ReferentialIntegrityValidation,
    rule_id="REFERENTIAL_INTEGRITY_CHECK",
    kind="formal",
    datasets_params={
        "grid.egon_etrago_load_timeseries": {
            "foreign_column": "load_id",
            "reference_dataset": "grid.egon_etrago_load", 
            "reference_column": "load_id",
            "additional_conditions": "child.load_id IS NOT NULL"
        },
        "supply.egon_power_plants": {
            "foreign_column": "bus",
            "reference_dataset": "grid.egon_etrago_bus",
            "reference_column": "bus_id", 
            "additional_conditions": "child.bus IS NOT NULL"
        }
    }
)


@register(task="adhoc", dataset="grid.egon_etrago_load", rule_id="MULTI_COLUMN_REFERENTIAL_INTEGRITY",
          kind="formal", foreign_columns=["scn_name", "bus"], 
          reference_dataset="grid.egon_etrago_bus", reference_columns=["scn_name", "bus_id"])
class MultiColumnReferentialIntegrityValidation(SqlRule):
    """Validates referential integrity with multiple columns (composite foreign keys)."""
    
    def sql(self, ctx):
        foreign_cols = self.params.get("foreign_columns", ["id"])
        reference_dataset = self.params.get("reference_dataset")
        reference_cols = self.params.get("reference_columns", ["id"])
        scenario_col = self.params.get("scenario_col")
        
        if len(foreign_cols) != len(reference_cols):
            raise ValueError("foreign_columns and reference_columns must have same length")
        
        # Build join conditions
        join_conditions = []
        for f_col, r_col in zip(foreign_cols, reference_cols):
            join_conditions.append(f"child.{f_col} = parent.{r_col}")
        
        join_condition = " AND ".join(join_conditions)
        
        # Build null check conditions  
        null_checks = []
        for f_col in foreign_cols:
            null_checks.append(f"child.{f_col} IS NOT NULL")
        null_condition = " AND ".join(null_checks)
        
        base_query = f"""
        SELECT
            count(*) AS total_rows,
            count(CASE WHEN {null_condition} THEN 1 END) AS non_null_references,
            count(parent.{reference_cols[0]}) AS valid_references,
            count(CASE WHEN {null_condition} AND parent.{reference_cols[0]} IS NULL THEN 1 END) AS orphaned_references
        FROM
            {self.dataset} as child
        LEFT JOIN
            {reference_dataset} as parent
        ON {join_condition}
        """
        
        if ctx.scenario and scenario_col:
            base_query += f" WHERE child.{scenario_col} = :scenario"
            
        return base_query

    def postprocess(self, row, ctx):
        total_rows = int(row.get("total_rows") or 0)
        non_null_references = int(row.get("non_null_references") or 0)
        valid_references = int(row.get("valid_references") or 0)
        orphaned_references = int(row.get("orphaned_references") or 0)
        
        ok = (orphaned_references == 0)
        
        foreign_cols = self.params.get("foreign_columns", ["id"])
        reference_dataset = self.params.get("reference_dataset")
        reference_cols = self.params.get("reference_columns", ["id"])
        
        col_pairs = ", ".join([f"{f}→{r}" for f, r in zip(foreign_cols, reference_cols)])
        
        if ok:
            message = f"All {non_null_references} composite references ({col_pairs}) have valid matches in {reference_dataset}"
        else:
            message = f"{orphaned_references} orphaned composite references found ({col_pairs})"
        
        return RuleResult(
            rule_id=self.rule_id, task=self.task, dataset=self.dataset,
            success=ok, observed=float(orphaned_references), expected=0.0,
            message=message, severity=Severity.WARNING,
            schema=self.schema, table=self.table
        )