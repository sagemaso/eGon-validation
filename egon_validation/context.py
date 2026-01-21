"""Execution context for validation runs."""

import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
from egon_validation.logging_config import get_logger

logger = get_logger("context")


@dataclass
class RunContext:
    """Validation run context with run_id, output directory, and extra data."""

    run_id: str
    out_dir: Path = Path("validation_runs")
    extra: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    source: str = "manual"  # manual, airflow, api, etc.


class RunContextFactory:
    """Factory for creating standardized RunContext instances."""

    @staticmethod
    def create_timestamped(
        prefix: str = "run", out_dir: Optional[str] = None
    ) -> RunContext:
        """Create a RunContext with timestamp-based ID.

        Args:
            prefix: Prefix for the run ID
            out_dir: Output directory override

        Returns:
            RunContext with timestamped run_id
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"{prefix}_{timestamp}"

        output_dir = Path(out_dir) if out_dir else Path("validation_runs")

        logger.info(
            f"Created timestamped RunContext: {run_id}",
            extra={"run_id": run_id, "source": "manual"},
        )

        return RunContext(
            run_id=run_id,
            out_dir=output_dir,
            source="manual",
            extra={"created_by": "RunContextFactory.create_timestamped"},
        )

    @staticmethod
    def create_airflow(
        dag_id: str,
        execution_date: datetime,
        task_id: Optional[str] = None,
        out_dir: Optional[str] = None,
    ) -> RunContext:
        """Create a RunContext for Airflow execution.

        Args:
            dag_id: Airflow DAG identifier
            execution_date: Airflow execution date
            task_id: Optional task identifier
            out_dir: Output directory override

        Returns:
            RunContext with Airflow-specific metadata
        """
        # Format execution date for filename compatibility
        date_str = execution_date.strftime("%Y%m%d_%H%M%S")

        if task_id:
            run_id = f"airflow_{dag_id}_{task_id}_{date_str}"
        else:
            run_id = f"airflow_{dag_id}_{date_str}"

        # Airflow logs typically go to specific directories
        if out_dir:
            output_dir = Path(out_dir)
        else:
            # Default Airflow-style path
            output_dir = Path("/opt/airflow/logs/egon_validation") / dag_id / date_str

        logger.info(
            f"Created Airflow RunContext: {run_id}",
            extra={"run_id": run_id, "dag_id": dag_id, "source": "airflow"},
        )

        return RunContext(
            run_id=run_id,
            out_dir=output_dir,
            source="airflow",
            extra={
                "dag_id": dag_id,
                "execution_date": execution_date.isoformat(),
                "task_id": task_id,
                "created_by": "RunContextFactory.create_airflow",
            },
        )

    @staticmethod
    def create_unique(prefix: str = "run", out_dir: Optional[str] = None) -> RunContext:
        """Create a RunContext with UUID-based ID for guaranteed uniqueness.

        Args:
            prefix: Prefix for the run ID
            out_dir: Output directory override

        Returns:
            RunContext with UUID-based run_id
        """
        unique_id = str(uuid.uuid4())[:8]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"{prefix}_{timestamp}_{unique_id}"

        output_dir = Path(out_dir) if out_dir else Path("validation_runs")

        logger.info(
            f"Created unique RunContext: {run_id}",
            extra={"run_id": run_id, "source": "api"},
        )

        return RunContext(
            run_id=run_id,
            out_dir=output_dir,
            source="api",
            extra={
                "created_by": "RunContextFactory.create_unique",
                "uuid": unique_id,
            },
        )

    @staticmethod
    def create_from_environment() -> RunContext:
        """Create a RunContext based on environment variables.

        Looks for:
        - EGON_RUN_ID: explicit run ID
        - AIRFLOW_CTX_DAG_ID: Airflow DAG ID
        - AIRFLOW_CTX_EXECUTION_DATE: Airflow execution date
        - EGON_OUT_DIR: output directory

        Returns:
            RunContext configured from environment
        """
        # Check for explicit run ID
        run_id = os.getenv("EGON_RUN_ID")
        if run_id:
            output_dir = Path(os.getenv("EGON_OUT_DIR", "validation_runs"))
            logger.info(f"Created RunContext from EGON_RUN_ID: {run_id}")
            return RunContext(
                run_id=run_id,
                out_dir=output_dir,
                source="environment",
                extra={"created_by": "RunContextFactory.create_from_environment"},
            )

        # Check for Airflow context
        dag_id = os.getenv("AIRFLOW_CTX_DAG_ID")
        if dag_id:
            exec_date_str = os.getenv("AIRFLOW_CTX_EXECUTION_DATE")
            if exec_date_str:
                exec_date = datetime.fromisoformat(exec_date_str.replace("Z", "+00:00"))
            else:
                exec_date = datetime.now()

            task_id = os.getenv("AIRFLOW_CTX_TASK_ID")
            out_dir = os.getenv("EGON_OUT_DIR")

            return RunContextFactory.create_airflow(dag_id, exec_date, task_id, out_dir)

        # Fallback to timestamped
        out_dir = os.getenv("EGON_OUT_DIR")
        return RunContextFactory.create_timestamped("env_run", out_dir)
