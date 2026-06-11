import time
from itertools import chain

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from src.config.settings import DatabricksSettings


class DatabricksUtils:
    _databricks_client: WorkspaceClient | None = None
    _warehouse_id: str | None = None

    def __init__(self) -> None:
        """
        Initialize Databricks utilities.

        self.databricks_client = None
        self.warehouse_id = None
        """

        # Initialize the client and warehouse_id
        self._setup_databricks_workspace_client()

    def _setup_databricks_workspace_client(self):
        """
        Lazily initialize Databricks SQL client.
        """
        if not self._databricks_client:
            self._databricks_client = DatabricksSettings().initialize_databricks_workspace_client()
            self._warehouse_id = DatabricksSettings().get_warehouse_id()

    def resolve_databricks_query(self, sql_query: str):
        """
        Process the Databricks Delta Table configuration to execute the query and resolve its values.

        This method:
        1. Initializes the Databricks workspace client if not already done
        2. Generates a SQL query from the configuration
        3. Executes the query non-blocking (wait_timeout="0") to get a statement_id
        4. Polls the statement status with 1-second intervals until completion or timeout
        5. Retrieves all result data, including paginated chunks
        6. Returns the aggregated result data as a list

        Polling timeout is fixed at 5 minutes. The method handles multi-chunk results
        automatically by iterating through next_chunk_index values until all data is retrieved.

        Args:
            config: Databricks Delta Table configuration containing catalog, schema,
                   table name, and optional filters/field selections

        Returns:
            list: Aggregated result data from the Databricks query execution

        Raises:
            ValueError: If statement_id is not obtained, query fails, execution times out,
                       or no data is returned
        """

        if not self._databricks_client or not self._warehouse_id:
            raise ValueError("Databricks client is not initialized.")

        try:
            response = self._databricks_client.statement_execution.execute_statement(
                warehouse_id=self._warehouse_id,
                statement=sql_query,
                wait_timeout="0s",  # instant return with statement id only
            )

            # Poll for completion and fetch results
            statement_id = response.statement_id

            if not statement_id:
                raise ValueError("Failed to obtain statement ID from Databricks response.")

            # Poll for completion with 5-minute timeout
            start_time = time.time()
            timeout_seconds = 5 * 60  # 5 minutes

            while True:
                elapsed_time = time.time() - start_time
                if elapsed_time > timeout_seconds:
                    raise ValueError(
                        f"Databricks query execution timed out after {timeout_seconds} seconds"
                    )

                status_response = self._databricks_client.statement_execution.get_statement(
                    statement_id=statement_id
                )

                if (
                    status_response.status
                    and status_response.status.state
                    and status_response.status.state.value
                    in [
                        "SUCCEEDED",
                        "FAILED",
                        "CANCELED",
                        "CLOSED",
                    ]
                ):
                    break

                time.sleep(1)  # Wait before polling again

            # Check if execution succeeded
            if status_response.status.state != StatementState.SUCCEEDED:
                raise ValueError(
                    f"Databricks query failed with state: {status_response.status.state}"
                )

            # Extract initial data from status response
            if not status_response.result or not status_response.result.data_array:
                raise ValueError("No data returned from Databricks query")

            """
            Two Cases that can arise here:
                1. The query SELECTs a single column, so we flatten the List[List] like [[1], [2]] to [1, 2].
                2. The query SELECTs multiple columns, so we return a list of row dicts keyed by column name
                   (e.g. [{"store": "a", "gtin": 1}, ...]) using the result manifest schema. Column names let
                   downstream features (correlated zip, databricks lookups) reference columns by name. When the
                   manifest is unavailable, we fall back to row lists so resolution never crashes.
            """

            column_names = self._extract_column_names(status_response)
            should_flatten = len(status_response.result.data_array[0]) == 1

            result_data = self._shape_rows(
                status_response.result.data_array, should_flatten, column_names
            )

            # Check for additional chunks and retrieve them
            next_chunk_index = getattr(status_response.result, "next_chunk_index", None)

            while next_chunk_index is not None:
                chunk_response = (
                    self._databricks_client.statement_execution.get_statement_result_chunk_n(
                        statement_id=statement_id, chunk_index=next_chunk_index
                    )
                )

                if chunk_response.data_array:
                    result_data.extend(
                        self._shape_rows(
                            chunk_response.data_array,  # pyright: ignore
                            should_flatten,
                            column_names,
                        )
                    )

                next_chunk_index = getattr(chunk_response, "next_chunk_index", None)

            return result_data

        except Exception as e:
            raise ValueError(f"Failed to execute Databricks query: {e!s}") from e

    @staticmethod
    def _extract_column_names(status_response) -> list[str] | None:
        """
        Read column names from the statement result manifest schema.

        Returns None when the manifest, schema, or any column name is unavailable so callers
        can fall back to positional row lists instead of failing.
        """
        manifest = getattr(status_response, "manifest", None)
        schema = getattr(manifest, "schema", None)
        columns = getattr(schema, "columns", None)
        if not columns:
            return None

        names: list[str] = []
        for column in columns:
            name = getattr(column, "name", None)
            if name is None:
                return None
            names.append(name)
        return names

    @staticmethod
    def _shape_rows(data_array: list, should_flatten: bool, column_names: list[str] | None) -> list:
        """
        Shape a raw ``data_array`` chunk into the resolved result form.

        - Single-column queries flatten ``[[v], ...]`` to ``[v, ...]``.
        - Multi-column queries become row dicts keyed by column name when names are available;
          otherwise they degrade to row lists.
        """
        if should_flatten:
            return list(chain.from_iterable(data_array))
        if column_names and data_array and len(column_names) == len(data_array[0]):
            return [dict(zip(column_names, row, strict=False)) for row in data_array]
        return [list(row) for row in data_array]
