import pandas as pd
from pathlib import Path

from egon_validation.rules.base import Rule, RuleResult, Severity
from egon_validation.rules.registry import register
from egon_validation.config import EUROPE_LAT_BOUNDS, EUROPE_LON_BOUNDS
from egon_validation.utils.gas_data import ensure_gas_data

@register(task="sanity", dataset="gas.IGGIELGN_Nodes", rule_id="GAS_ABROAD_SANITY",
          kind="sanity")
class GasAbroadSanity(Rule):
    """
    Sanity check for gas network nodes abroad (non-German).
    
    Validates gas network nodes from neighboring countries in the 
    SciGRID_gas IGGIELGN dataset.
    Based on etrago_eGon2035_gas_abroad() from eGon-data sanity_checks.py.
    
    Reads directly from IGGIELGN_Nodes.csv file.
    """
    
    def evaluate(self, engine, ctx):
        try:
            # Ensure gas data is downloaded
            gas_data_dir = ensure_gas_data()
            nodes_file = gas_data_dir / "IGGIELGN_Nodes.csv"
            
            if not nodes_file.exists():
                return RuleResult(
                    rule_id=self.rule_id, task=self.task, dataset=self.dataset,
                    success=False, observed=None, expected=None,
                    message=f"Gas nodes file not found: {nodes_file}",
                    severity=Severity.ERROR, schema="gas", table="IGGIELGN_Nodes"
                )
            
            # Read gas nodes data (based on original sanity check logic)
            df = pd.read_csv(nodes_file, delimiter=";", decimal=".")
            
            # Filter for non-German (abroad) nodes (based on original logic from line 1506)
            if 'country_code' in df.columns:
                abroad_df = df[~df['country_code'].str.match("DE", na=False)]
            else:
                # If no country_code, cannot determine foreign nodes
                return RuleResult(
                    rule_id=self.rule_id, task=self.task, dataset=self.dataset,
                    success=False, observed=None, expected=None,
                    message="No country_code column found in gas nodes data",
                    severity=Severity.ERROR, schema="gas", table="IGGIELGN_Nodes"
                )
            
            total_abroad_nodes = len(abroad_df)
            
            # Analyze abroad gas nodes
            issues = []
            
            if total_abroad_nodes == 0:
                issues.append("No gas nodes from abroad (neighboring countries) found")
            
            # Check for required columns and data quality
            expected_cols = ['lat', 'long', 'id', 'country_code']
            missing_cols = [col for col in expected_cols if col not in abroad_df.columns]
            if missing_cols:
                issues.append(f"Missing columns: {missing_cols}")
            
            # Check for data completeness
            if 'id' in abroad_df.columns:
                null_ids = abroad_df['id'].isnull().sum()
                if null_ids > 0:
                    issues.append(f"{null_ids} abroad nodes with missing IDs")
            
            if 'lat' in abroad_df.columns and 'long' in abroad_df.columns:
                invalid_coords = abroad_df[
                    (abroad_df['lat'].isnull()) | (abroad_df['long'].isnull()) |
                    (abroad_df['lat'] < EUROPE_LAT_BOUNDS[0]) | (abroad_df['lat'] > EUROPE_LAT_BOUNDS[1]) |
                    (abroad_df['long'] < EUROPE_LON_BOUNDS[0]) | (abroad_df['long'] > EUROPE_LON_BOUNDS[1])
                ].shape[0]
                if invalid_coords > 0:
                    issues.append(f"{invalid_coords} abroad nodes with invalid/missing coordinates")
            
            # Check country distribution
            if 'country_code' in abroad_df.columns:
                countries = abroad_df['country_code'].value_counts()
                if len(countries) == 0:
                    issues.append("No valid country codes for abroad nodes")
            
            ok = len(issues) == 0
            
            if ok:
                countries_str = ""
                if 'country_code' in abroad_df.columns:
                    countries = abroad_df['country_code'].value_counts()
                    countries_str = f" across {len(countries)} countries: {dict(countries)}"
                    
                message = f"Gas abroad nodes reasonable: {total_abroad_nodes} nodes{countries_str}"
            else:
                message = f"Gas abroad nodes issues: {'; '.join(issues)}"
                
            return RuleResult(
                rule_id=self.rule_id, task=self.task, dataset=self.dataset,
                success=ok, observed=len(issues), expected=0.0,
                message=message, severity=Severity.WARNING,
                schema="gas", table="IGGIELGN_Nodes"
            )
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            return RuleResult(
                rule_id=self.rule_id, task=self.task, dataset=self.dataset,
                success=False, observed=None, expected=None,
                message=f"Rule execution failed: {e}\nDetails: {error_details}",
                severity=Severity.ERROR, schema="gas", table="IGGIELGN_Nodes"
            )