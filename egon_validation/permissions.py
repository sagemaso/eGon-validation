"""Database permission validation system for PostgreSQL."""

from typing import Dict, List, Tuple
from sqlalchemy.engine import Engine
from egon_validation.logging_config import get_logger
from egon_validation.exceptions import (
    PermissionDeniedError,
    DatabaseConnectionError,
)
from egon_validation.retry import database_retry
from egon_validation import db

logger = get_logger("permissions")


class PermissionValidator:
    """Validates database permissions before executing queries."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self._permission_cache: Dict[str, bool] = {}

    @database_retry
    def check_table_access(
        self, schema: str, table: str, privilege: str = "SELECT"
    ) -> bool:
        """
        Check if current user has specified privilege on a table.

        Args:
            schema: Database schema name
            table: Table name
            privilege: Required privilege (SELECT, INSERT, UPDATE, DELETE)

        Returns:
            True if permission exists, False otherwise

        Raises:
            PermissionDeniedError: If permission is denied
            DatabaseConnectionError: If unable to check permissions
        """
        cache_key = f"{schema}.{table}.{privilege}"

        # Check cache first
        if cache_key in self._permission_cache:
            logger.debug(f"Using cached permission result for {cache_key}")
            return self._permission_cache[cache_key]

        try:
            # Query PostgreSQL system tables for privileges
            permission_query = """
            SELECT has_table_privilege(current_user, %s, %s) as has_privilege
            """
            table_name = f'"{schema}"."{table}"'

            result = db.fetch_one(
                self.engine,
                permission_query,
                {"table": table_name, "privilege": privilege},
            )
            has_permission = result.get("has_privilege", False)

            # Cache the result
            self._permission_cache[cache_key] = has_permission

            if has_permission:
                logger.debug(f"Permission granted: {privilege} on {schema}.{table}")
            else:
                logger.warning(f"Permission denied: {privilege} on {schema}.{table}")

            return has_permission

        except Exception as e:
            logger.error(
                f"Failed to check table permission: {str(e)}",
                extra={
                    "schema": schema,
                    "table": table,
                    "privilege": privilege,
                    "error": str(e),
                },
            )
            raise DatabaseConnectionError(
                f"Unable to check permissions: {str(e)}"
            ) from e

    @database_retry
    def check_schema_access(self, schema: str, privilege: str = "USAGE") -> bool:
        """
        Check if current user has privilege on a schema.

        Args:
            schema: Database schema name
            privilege: Required privilege (USAGE, CREATE)

        Returns:
            True if permission exists, False otherwise
        """
        cache_key = f"schema.{schema}.{privilege}"

        if cache_key in self._permission_cache:
            return self._permission_cache[cache_key]

        try:
            schema_query = """
            SELECT has_schema_privilege(current_user, %s, %s) as has_privilege
            """

            result = db.fetch_one(
                self.engine,
                schema_query,
                {"schema": schema, "privilege": privilege},
            )
            has_permission = result.get("has_privilege", False)

            self._permission_cache[cache_key] = has_permission

            if has_permission:
                logger.debug(f"Schema permission granted: {privilege} on {schema}")
            else:
                logger.warning(f"Schema permission denied: {privilege} on {schema}")

            return has_permission

        except Exception as e:
            logger.error(
                f"Failed to check schema permission: {str(e)}",
                extra={
                    "schema": schema,
                    "privilege": privilege,
                    "error": str(e),
                },
            )
            raise DatabaseConnectionError(
                f"Unable to check schema permissions: {str(e)}"
            ) from e

    @database_retry
    def check_system_table_access(self) -> Dict[str, bool]:
        """
        Check access to PostgreSQL system tables used by validation rules.

        Returns:
            Dictionary mapping system table names to access status
        """
        system_tables = [
            "information_schema.tables",
            "information_schema.columns",
            "information_schema.table_constraints",
            "pg_catalog.pg_tables",
            "pg_catalog.pg_class",
            "pg_catalog.pg_namespace",
        ]

        results = {}

        for table in system_tables:
            try:
                # Try to query the system table with LIMIT 1
                test_query = f"SELECT 1 FROM {table} LIMIT 1"
                db.fetch_one(self.engine, test_query)
                results[table] = True
                logger.debug(f"System table accessible: {table}")
            except Exception as e:
                results[table] = False
                logger.warning(f"System table access denied: {table} - {str(e)}")

        return results

    def validate_required_permissions(
        self,
        required_tables: List[Tuple[str, str]],
        fail_on_missing: bool = True,
    ) -> Dict[str, bool]:
        """
        Validate permissions for a list of required tables.

        Args:
            required_tables: List of (schema, table) tuples
            fail_on_missing: Raise exception if any permission is missing

        Returns:
            Dictionary mapping table names to permission status

        Raises:
            PermissionDeniedError: If fail_on_missing=True and permissions missing
        """
        results = {}
        missing_permissions = []

        for schema, table in required_tables:
            table_key = f"{schema}.{table}"
            try:
                has_permission = self.check_table_access(schema, table, "SELECT")
                results[table_key] = has_permission

                if not has_permission:
                    missing_permissions.append(table_key)

            except DatabaseConnectionError:
                results[table_key] = False
                missing_permissions.append(table_key)

        if fail_on_missing and missing_permissions:
            error_msg = (
                f"Missing permissions for tables: {', '.join(missing_permissions)}"
            )
            logger.error(error_msg, extra={"missing_tables": missing_permissions})
            raise PermissionDeniedError(error_msg)

        return results

    @database_retry
    def get_accessible_tables(self, schema: str) -> List[str]:
        """
        Get list of tables in schema that current user can access.

        Args:
            schema: Database schema name

        Returns:
            List of accessible table names
        """
        try:
            # First check schema access
            if not self.check_schema_access(schema, "USAGE"):
                logger.warning(f"No USAGE permission on schema {schema}")
                return []

            # Query for accessible tables
            tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND has_table_privilege(
                current_user, table_schema||'.'||table_name, 'SELECT'
            )
            ORDER BY table_name
            """

            rows = db.fetch_all(self.engine, tables_query, {"schema": schema})
            accessible_tables = [row["table_name"] for row in rows]

            logger.info(
                f"Found {len(accessible_tables)} accessible tables in schema {schema}"
            )
            return accessible_tables

        except Exception as e:
            logger.error(
                f"Failed to get accessible tables: {str(e)}",
                extra={"schema": schema, "error": str(e)},
            )
            return []

    def get_user_info(self) -> Dict[str, str]:
        """
        Get current database user information.

        Returns:
            Dictionary with user info (name, database, etc.)
        """
        try:
            user_query = """
            SELECT
                current_user as username,
                current_database() as database,
                session_user as session_user,
                version() as postgres_version
            """

            result = db.fetch_one(self.engine, user_query)
            logger.info("Database user info retrieved", extra=result)
            return result

        except Exception as e:
            logger.error(f"Failed to get user info: {str(e)}", extra={"error": str(e)})
            return {"username": "unknown", "database": "unknown"}

    def clear_cache(self) -> None:
        """Clear the permission cache."""
        self._permission_cache.clear()
        logger.debug("Permission cache cleared")


def check_validation_permissions(engine: Engine, schemas: List[str]) -> Dict[str, any]:
    """
    Comprehensive permission check for validation framework.

    Args:
        engine: Database engine
        schemas: List of schemas to validate

    Returns:
        Dictionary with permission check results
    """
    validator = PermissionValidator(engine)

    results = {
        "user_info": validator.get_user_info(),
        "system_tables": validator.check_system_table_access(),
        "schemas": {},
        "summary": {
            "total_schemas": len(schemas),
            "accessible_schemas": 0,
            "total_tables": 0,
            "accessible_tables": 0,
        },
    }

    for schema in schemas:
        schema_info = {
            "has_usage": validator.check_schema_access(schema, "USAGE"),
            "accessible_tables": [],
        }

        if schema_info["has_usage"]:
            results["summary"]["accessible_schemas"] += 1
            schema_info["accessible_tables"] = validator.get_accessible_tables(schema)
            results["summary"]["accessible_tables"] += len(
                schema_info["accessible_tables"]
            )

        results["schemas"][schema] = schema_info
        results["summary"]["total_tables"] += len(schema_info["accessible_tables"])

    logger.info(
        f"Permission check completed: "
        f"{results['summary']['accessible_schemas']}/"
        f"{results['summary']['total_schemas']} schemas, "
        f"{results['summary']['accessible_tables']} accessible tables"
    )

    return results
