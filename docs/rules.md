# Built-in Rules

## Data Quality Rules

| Rule | Purpose | Key Parameters |
|------|---------|----------------|
| `NotNullAndNotNaNValidation` | No NULL/NaN in specified columns | `columns` |
| `WholeTableNotNullAndNotNaNValidation` | No NULL/NaN in any column | - |
| `DataTypeValidation` | Verify column data types | `columns`, `expected_types` |
| `ValueSetValidation` | Values in allowed set | `column`, `allowed_values` |
| `RowCountValidation` | Row count within bounds | `min_rows`, `max_rows` |
| `ArrayCardinalityValidation` | Array length constraints | `column`, `min_length`, `max_length` |

## Referential Integrity

| Rule | Purpose | Key Parameters |
|------|---------|----------------|
| `ReferentialIntegrityValidation` | Foreign key validation | `fk_column`, `ref_table`, `ref_column` |

## PostGIS Rules

| Rule | Purpose | Key Parameters |
|------|---------|----------------|
| `GeometryContainmentValidation` | Geometry validity & containment | `geom_column`, `boundary_wkt` |
| `SRIDUniqueNonZero` | SRID is unique and non-zero | `geom_column` |
| `SRIDSpecificValidation` | SRID matches expected value | `geom_column`, `expected_srid` |

## Rule Registration

Rules are registered with the `@register` decorator:

```python
@register(
    task="data_quality",
    table="schema.table_name",
    rule_id="MY_RULE",
    kind="formal",
    # rule-specific params...
)
class MyRule(SqlRule):
    ...
```