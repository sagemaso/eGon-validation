#!/bin/bash

# Convenience script for running eGon validations with SSH tunnel

set -e

# Default values
TASK="validation-test"
SCENARIO=""
RUN_ID="validation-$(date +%Y%m%d%H%M%S)"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --task)
            TASK="$2"
            shift 2
            ;;
        --scenario)
            SCENARIO="$2"
            shift 2
            ;;
        --run-id)
            RUN_ID="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [--task TASK] [--scenario SCENARIO] [--run-id RUN_ID]"
            echo ""
            echo "Options:"
            echo "  --task TASK        Task to run (default: validation-test)"
            echo "  --scenario SCENARIO Scenario name (optional)"
            echo "  --run-id RUN_ID    Run identifier (default: validation-YYYYMMDDHHMMSS)"
            echo "  --help, -h         Show this help message"
            echo ""
            echo "This script automatically uses SSH tunnel if configured in .env file."
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

echo "🚀 Running eGon validation with SSH tunnel..."
echo "   Task: $TASK"
echo "   Run ID: $RUN_ID"
if [[ -n "$SCENARIO" ]]; then
    echo "   Scenario: $SCENARIO"
fi
echo ""

# Build command
CMD="python -m egon_validation.cli run-task --run-id \"$RUN_ID\" --task \"$TASK\" --with-tunnel"
if [[ -n "$SCENARIO" ]]; then
    CMD="$CMD --scenario \"$SCENARIO\""
fi

# Run validation
echo "💾 Running validation..."
eval $CMD

# Generate report
echo ""
echo "📊 Generating final report..."
python -m egon_validation.cli final-report --run-id "$RUN_ID"

# Show summary
echo ""
echo "✅ Validation complete!"
echo "   Report: ./validation_runs/$RUN_ID/final/report.html"
echo "   Raw results: ./validation_runs/$RUN_ID/tasks/$TASK/results.jsonl"