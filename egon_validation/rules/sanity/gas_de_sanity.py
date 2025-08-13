import pandas as pd
from pathlib import Path

from egon_validation.rules.base import Rule, RuleResult, Severity
from egon_validation.rules.registry import register
from egon_validation.utils.gas_data import ensure_gas_data

@register(task="adhoc", dataset="gas.IGGIELGN_Productions", rule_id="GAS_DE_SANITY",
          kind="sanity")
class GasDeSanity(Rule):
    """
    Sanity check for domestic gas production data in Germany.
    
    Validates German gas production sites from SciGRID_gas IGGIELGN dataset.
    Based on etrago_eGon2035_gas_DE() from eGon-data sanity_checks.py.
    
    Reads directly from IGGIELGN_Productions.csv file.
    """
    
    def evaluate(self, engine, ctx):
        try:
            # Ensure gas data is downloaded
            gas_data_dir = ensure_gas_data()
            productions_file = gas_data_dir / "IGGIELGN_Productions.csv"
            
            if not productions_file.exists():
                return RuleResult(
                    rule_id=self.rule_id, task=self.task, dataset=self.dataset,
                    success=False, observed=None, expected=None,
                    message=f"Gas productions file not found: {productions_file}",
                    severity=Severity.ERROR, schema="gas", table="IGGIELGN_Productions"
                )
            
            # Read gas productions data (based on original sanity check logic)
            df = pd.read_csv(productions_file, delimiter=";", decimal=".")
            
            # Filter for German production sites (based on original logic)
            if 'country_code' in df.columns:
                german_df = df[df['country_code'].str.match("DE", na=False)]
            else:
                # Fallback: assume all sites are German if no country_code
                german_df = df
            
            total_sites = len(german_df)
            
            # Analyze production data
            issues = []
            
            if total_sites == 0:
                issues.append("No German gas production sites found")
            
            # Check for required columns and data quality
            expected_cols = ['lat', 'long', 'id']
            missing_cols = [col for col in expected_cols if col not in german_df.columns]
            if missing_cols:
                issues.append(f"Missing columns: {missing_cols}")
            
            # Check for data completeness
            if 'id' in german_df.columns:
                null_ids = german_df['id'].isnull().sum()
                if null_ids > 0:
                    issues.append(f"{null_ids} production sites with missing IDs")
            
            if 'lat' in german_df.columns and 'long' in german_df.columns:
                invalid_coords = german_df[
                    (german_df['lat'].isnull()) | (german_df['long'].isnull()) |
                    (german_df['lat'] < 45) | (german_df['lat'] > 55) |  # Rough Germany bounds
                    (german_df['long'] < 5) | (german_df['long'] > 15)
                ].shape[0]
                if invalid_coords > 0:
                    issues.append(f"{invalid_coords} sites with invalid/missing coordinates")
            
            ok = len(issues) == 0
            
            if ok:
                unique_types = german_df.get('type', pd.Series()).nunique() if 'type' in german_df.columns else 0
                message = f"German gas production data reasonable: {total_sites} production sites"
                if unique_types > 0:
                    message += f" ({unique_types} types)"
            else:
                message = f"German gas production issues: {'; '.join(issues)}"
                
            return RuleResult(
                rule_id=self.rule_id, task=self.task, dataset=self.dataset,
                success=ok, observed=len(issues), expected=0.0,
                message=message, severity=Severity.WARNING,
                schema="gas", table="IGGIELGN_Productions"
            )
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            return RuleResult(
                rule_id=self.rule_id, task=self.task, dataset=self.dataset,
                success=False, observed=None, expected=None,
                message=f"Rule execution failed: {e}\nDetails: {error_details}",
                severity=Severity.ERROR, schema="gas", table="IGGIELGN_Productions"
            )