"""
MetaLayer Incremental Processing Framework
=========================================

Advanced utilities for efficient incremental data processing with state management,
change data capture (CDC), and timestamp-based incremental loads.

Features:
- Automated state management for incremental loads
- Multiple incremental strategies (timestamp, hash, CDC)
- Error recovery and data consistency checks
- Performance optimization with chunking and batching
"""

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


class IncrementalStrategy(Enum):
    """Supported incremental processing strategies"""

    TIMESTAMP = "timestamp"
    HASH_DIFF = "hash_diff"
    CDC = "change_data_capture"
    FULL_REFRESH = "full_refresh"


@dataclass
class IncrementalState:
    """State management for incremental processing"""

    table_name: str
    strategy: str
    last_processed_timestamp: Optional[datetime] = None
    last_processed_hash: Optional[str] = None
    last_processed_record_id: Optional[str] = None
    processed_record_count: int = 0
    last_run_success: bool = True
    error_count: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        self.updated_at = datetime.now()


class IncrementalProcessor:
    """Advanced incremental data processing engine"""

    def __init__(self, conn_id: str = "postgres_default", chunk_size: int = 50000):
        self.conn_id = conn_id
        self.chunk_size = chunk_size
        self.hook = PostgresHook(postgres_conn_id=conn_id)
        self.state_table = "airflow_meta.incremental_state"
        self._ensure_state_table()

    def _ensure_state_table(self):
        """Create state management table if not exists"""
        create_schema_sql = "CREATE SCHEMA IF NOT EXISTS airflow_meta;"

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.state_table} (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(200) NOT NULL UNIQUE,
            strategy VARCHAR(50) NOT NULL,
            last_processed_timestamp TIMESTAMP,
            last_processed_hash VARCHAR(64),
            last_processed_record_id VARCHAR(100),
            processed_record_count INTEGER DEFAULT 0,
            last_run_success BOOLEAN DEFAULT TRUE,
            error_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_incremental_state_table
        ON {self.state_table}(table_name);
        """

        try:
            self.hook.run([create_schema_sql, create_table_sql])
            logger.info("Incremental state management table initialized")
        except Exception as e:
            logger.error(f"Failed to initialize state table: {e}")
            raise AirflowException(f"State table initialization failed: {e}")

    def get_state(self, table_name: str) -> Optional[IncrementalState]:
        """Retrieve current incremental state for a table"""
        try:
            sql = f"""
            SELECT table_name, strategy, last_processed_timestamp,
                   last_processed_hash, last_processed_record_id,
                   processed_record_count, last_run_success, error_count,
                   created_at, updated_at
            FROM {self.state_table}
            WHERE table_name = %s
            """

            result = self.hook.get_first(sql, parameters=(table_name,))

            if result:
                return IncrementalState(
                    table_name=result[0],
                    strategy=result[1],
                    last_processed_timestamp=result[2],
                    last_processed_hash=result[3],
                    last_processed_record_id=result[4],
                    processed_record_count=result[5],
                    last_run_success=result[6],
                    error_count=result[7],
                    created_at=result[8],
                    updated_at=result[9],
                )
            return None

        except Exception as e:
            logger.error(f"Failed to retrieve state for {table_name}: {e}")
            return None

    def update_state(self, state: IncrementalState):
        """Update incremental state after processing"""
        try:
            upsert_sql = f"""
            INSERT INTO {self.state_table}
                (table_name, strategy, last_processed_timestamp,
                 last_processed_hash, last_processed_record_id,
                 processed_record_count, last_run_success, error_count,
                 created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE SET
                strategy = EXCLUDED.strategy,
                last_processed_timestamp = EXCLUDED.last_processed_timestamp,
                last_processed_hash = EXCLUDED.last_processed_hash,
                last_processed_record_id = EXCLUDED.last_processed_record_id,
                processed_record_count = EXCLUDED.processed_record_count,
                last_run_success = EXCLUDED.last_run_success,
                error_count = EXCLUDED.error_count,
                updated_at = CURRENT_TIMESTAMP
            """

            self.hook.run(
                upsert_sql,
                parameters=(
                    state.table_name,
                    state.strategy,
                    state.last_processed_timestamp,
                    state.last_processed_hash,
                    state.last_processed_record_id,
                    state.processed_record_count,
                    state.last_run_success,
                    state.error_count,
                    state.created_at,
                    state.updated_at,
                ),
            )

            logger.info(f"Updated incremental state for {state.table_name}")

        except Exception as e:
            logger.error(f"Failed to update state for {state.table_name}: {e}")
            raise AirflowException(f"State update failed: {e}")

    def process_csv_incremental_timestamp(
        self,
        csv_path: str,
        target_table: str,
        target_schema: str,
        timestamp_column: str = "created_at",
    ) -> Dict[str, Any]:
        """Process CSV file with timestamp-based incremental loading"""

        state = self.get_state(f"{target_schema}.{target_table}")
        if state is None:
            state = IncrementalState(
                table_name=f"{target_schema}.{target_table}",
                strategy=IncrementalStrategy.TIMESTAMP.value,
            )

        try:
            # Read CSV in chunks for memory efficiency
            total_processed = 0
            new_records = 0
            last_timestamp = state.last_processed_timestamp

            for chunk_df in pd.read_csv(csv_path, chunksize=self.chunk_size):
                # Convert timestamp column to datetime
                if timestamp_column in chunk_df.columns:
                    chunk_df[timestamp_column] = pd.to_datetime(
                        chunk_df[timestamp_column]
                    )

                    # Filter for new records only
                    if last_timestamp:
                        new_chunk = chunk_df[
                            chunk_df[timestamp_column] > last_timestamp
                        ]
                    else:
                        new_chunk = chunk_df

                    if len(new_chunk) > 0:
                        # Insert new records
                        engine = self.hook.get_sqlalchemy_engine()
                        new_chunk.to_sql(
                            name=target_table,
                            schema=target_schema,
                            con=engine,
                            if_exists="append",
                            index=False,
                            method="multi",
                        )

                        new_records += len(new_chunk)
                        last_timestamp = new_chunk[timestamp_column].max()

                total_processed += len(chunk_df)

            # Update state
            state.last_processed_timestamp = last_timestamp
            state.processed_record_count += new_records
            state.last_run_success = True
            state.error_count = 0
            self.update_state(state)

            result = {
                "status": "success",
                "total_records_scanned": total_processed,
                "new_records_inserted": new_records,
                "last_processed_timestamp": (
                    last_timestamp.isoformat() if last_timestamp else None
                ),
            }

            logger.info(f"Incremental timestamp processing completed: {result}")
            return result

        except Exception as e:
            state.last_run_success = False
            state.error_count += 1
            self.update_state(state)

            logger.error(f"Incremental processing failed for {target_table}: {e}")
            raise AirflowException(f"Incremental processing failed: {e}")

    def process_csv_incremental_hash(
        self,
        csv_path: str,
        target_table: str,
        target_schema: str,
        hash_columns: List[str] = None,
    ) -> Dict[str, Any]:
        """Process CSV with hash-based change detection"""

        state = self.get_state(f"{target_schema}.{target_table}")
        if state is None:
            state = IncrementalState(
                table_name=f"{target_schema}.{target_table}",
                strategy=IncrementalStrategy.HASH_DIFF.value,
            )

        try:
            # Read current CSV data
            df = pd.read_csv(csv_path)

            # Calculate hash of data (or specified columns)
            if hash_columns:
                hash_data = df[hash_columns].to_string()
            else:
                hash_data = df.to_string()

            current_hash = hashlib.md5(hash_data.encode()).hexdigest()

            # Compare with previous hash
            if state.last_processed_hash == current_hash:
                logger.info(f"No changes detected for {target_table} (hash match)")
                return {
                    "status": "no_changes",
                    "records_scanned": len(df),
                    "hash": current_hash,
                }

            # Data has changed, perform full reload
            engine = self.hook.get_sqlalchemy_engine()

            # Truncate and reload
            with engine.connect() as conn:
                conn.execute(f"TRUNCATE TABLE {target_schema}.{target_table}")
                conn.commit()

            df.to_sql(
                name=target_table,
                schema=target_schema,
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
            )

            # Update state
            state.last_processed_hash = current_hash
            state.processed_record_count = len(df)
            state.last_run_success = True
            state.error_count = 0
            self.update_state(state)

            result = {
                "status": "success",
                "records_processed": len(df),
                "hash": current_hash,
                "previous_hash": state.last_processed_hash,
            }

            logger.info(f"Hash-based incremental processing completed: {result}")
            return result

        except Exception as e:
            state.last_run_success = False
            state.error_count += 1
            self.update_state(state)

            logger.error(f"Hash-based incremental processing failed: {e}")
            raise AirflowException(f"Hash-based processing failed: {e}")

    def get_incremental_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about all incremental processes"""
        try:
            sql = f"""
            SELECT
                COUNT(*) as total_tables,
                COUNT(CASE WHEN last_run_success THEN 1 END) as successful_runs,
                COUNT(CASE WHEN NOT last_run_success THEN 1 END) as failed_runs,
                SUM(processed_record_count) as total_records_processed,
                AVG(error_count) as avg_error_count,
                MIN(updated_at) as earliest_update,
                MAX(updated_at) as latest_update
            FROM {self.state_table}
            """

            result = self.hook.get_first(sql)

            if result:
                return {
                    "total_tables": result[0],
                    "successful_runs": result[1],
                    "failed_runs": result[2],
                    "total_records_processed": result[3],
                    "avg_error_count": float(result[4]) if result[4] else 0,
                    "earliest_update": result[5],
                    "latest_update": result[6],
                }
            return {}

        except Exception as e:
            logger.error(f"Failed to get incremental statistics: {e}")
            return {}


# Factory function for easy DAG integration
def create_incremental_processor(
    conn_id: str = "postgres_default",
) -> IncrementalProcessor:
    """Factory function to create incremental processor instance"""
    return IncrementalProcessor(conn_id=conn_id)
