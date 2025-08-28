# eGon Validation Examples

This directory contains practical examples demonstrating various aspects of the eGon validation framework.

## Examples Overview

### 1. [basic_usage.py](basic_usage.py)
**Complete end-to-end validation workflow**

Demonstrates:
- Database connection setup
- Creating validation runs
- Executing rules and generating reports
- Result analysis and summary

```bash
# Run basic example
python examples/basic_usage.py
```

### 2. [custom_rule_example.py](custom_rule_example.py)
**Creating custom validation rules**

Shows how to implement:
- SQL-based custom rules
- Python-based custom rules
- Parameterized rules with register_map
- Domain-specific validation logic

```bash
# See available custom rules
python examples/custom_rule_example.py
```

### 3. [airflow_integration.py](airflow_integration.py)
**Apache Airflow integration patterns**

Covers:
- Airflow DAG configuration
- Python and Bash operator approaches  
- Error handling and thresholds
- Notification workflows

## Prerequisites

Before running examples, ensure:

1. **Database connection configured**:
   ```bash
   export DB_URL="postgresql://user:pass@host:port/db"
   # OR individual variables:
   export DB_HOST="localhost"
   export DB_PORT="5432" 
   export DB_NAME="egon_data"
   export DB_USER="postgres"
   export DB_PASSWORD="secret"
   ```

2. **Package installed**:
   ```bash
   pip install -e .
   ```

3. **Test data available** (for meaningful results)

## Configuration Examples

### Environment Configuration (.env file)
```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=egon_data
DB_USER=postgres
DB_PASSWORD=secret

# Validation settings
MAX_WORKERS=4
DEFAULT_TOLERANCE=0.01
OUTPUT_DIR=./validation_runs

# Airflow integration
MAX_FAILURE_RATE=0.1
NOTIFICATION_WEBHOOK=https://hooks.slack.com/...
```

### Custom Rule Registration
```python
from egon_validation.rules.base import SqlRule, RuleResult, Severity
from egon_validation.rules.registry import register

@register(
    task="my_task",
    dataset="schema.table", 
    rule_id="MY_RULE",
    kind="custom",
    column="value",
    threshold=100
)
class MyCustomRule(SqlRule):
    def sql(self, ctx):
        return f"SELECT COUNT(*) as count FROM {self.dataset}"
    
    def postprocess(self, row, ctx):
        count = int(row.get("count", 0))
        threshold = self.params.get("threshold")
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            dataset=self.dataset,
            success=(count >= threshold),
            observed=count,
            expected=f">={threshold}"
        )
```

## Troubleshooting

**Database connection issues**:
- Verify database credentials and network connectivity
- Check if SSH tunnel is required (`--with-tunnel` flag)
- Ensure PostgreSQL extensions (PostGIS) are available

**Rule execution failures**:
- Check table/schema names exist in database
- Verify column names in rule parameters
- Review SQL syntax in custom rules

**Performance issues**:
- Adjust `MAX_WORKERS` environment variable
- Optimize SQL queries in custom rules
- Consider database connection pooling settings

## Integration Patterns

### CI/CD Pipeline
```yaml
# .github/workflows/data-validation.yml
- name: Run Data Validation
  run: |
    export DB_URL="${{ secrets.DATABASE_URL }}"
    python examples/basic_usage.py
```

### Docker Deployment
```dockerfile
FROM python:3.8
COPY . /app
WORKDIR /app
RUN pip install -e .
CMD ["python", "examples/basic_usage.py"]
```

### Monitoring Integration
```python
# Send metrics to monitoring system
def send_metrics(results):
    total = len(results)
    failed = sum(1 for r in results if not r.success)
    
    # Send to Prometheus/InfluxDB/etc
    metrics_client.gauge('validation.total_rules', total)
    metrics_client.gauge('validation.failed_rules', failed)
```