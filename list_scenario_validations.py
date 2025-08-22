#!/usr/bin/env python3
"""
Script to list all validations that include scenarios.
"""
import sys
sys.path.insert(0, '/home/sarah/PycharmProjects/egon-validation')

from egon_validation.rules.registry import list_registered

def main():
    registered = list_registered()
    
    print("=== ALL VALIDATIONS WITH SCENARIOS ===\n")
    
    scenario_validations = []
    for rule in registered:
        if rule.get("scenario") is not None:
            scenario_validations.append(rule)
    
    if not scenario_validations:
        print("No validations found with explicit scenario parameter.\n")
    else:
        for i, rule in enumerate(scenario_validations, 1):
            print(f"{i}. Rule ID: {rule['rule_id']}")
            print(f"   Task: {rule['task']}")
            print(f"   Dataset: {rule['dataset']}")
            print(f"   Kind: {rule['kind']}")
            print(f"   Scenario: {rule['scenario']}")
            print(f"   Params: {rule['params']}")
            print()
    
    print("=== VALIDATIONS USING SCENARIO_COL PARAMETER ===\n")
    
    scenario_col_validations = []
    for rule in registered:
        params = rule.get("params", {})
        if "scenario_col" in params:
            scenario_col_validations.append(rule)
    
    if not scenario_col_validations:
        print("No validations found with scenario_col parameter.\n")
    else:
        for i, rule in enumerate(scenario_col_validations, 1):
            print(f"{i}. Rule ID: {rule['rule_id']}")
            print(f"   Task: {rule['task']}")
            print(f"   Dataset: {rule['dataset']}")
            print(f"   Kind: {rule['kind']}")
            scenario_col = rule['params'].get('scenario_col')
            print(f"   Scenario Column: {scenario_col}")
            print(f"   Params: {rule['params']}")
            print()
    
    print("=== SUMMARY ===")
    print(f"Total validations: {len(registered)}")
    print(f"With scenario parameter: {len(scenario_validations)}")
    print(f"With scenario_col parameter: {len(scenario_col_validations)}")

if __name__ == "__main__":
    main()