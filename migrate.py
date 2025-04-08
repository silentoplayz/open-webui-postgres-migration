# -*- coding: utf-8 -*-
import psycopg
import traceback
import sys
import sqlite3
import json # <-- Import JSON
from psycopg.types.json import Json, Jsonb # Import the Json adapter
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, SpinnerColumn, TimeElapsedColumn
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, IntPrompt, Confirm
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
import asyncio
from contextlib import asynccontextmanager

console = Console(record=True) # Enable recording for potential saving later

# Configuration
MAX_RETRIES = 3

# --- Configuration functions (Assume correct as provided previously) ---
def get_sqlite_config() -> Path:
    """Interactive configuration for SQLite database path"""
    console.print(Panel("SQLite Database Configuration", style="cyan"))
    default_path = 'webui.db'
    while True:
        db_path_str = Prompt.ask("[cyan]SQLite database path[/]", default=default_path)
        db_path = Path(db_path_str)
        if not db_path.exists():
            console.print(f"\n[red]Error: File '{db_path}' does not exist[/]")
            if not Confirm.ask("\n[yellow]Try different path?[/]"): console.print("[red]Cancelled[/]"); sys.exit(0)
            continue
        if not db_path.is_file():
            console.print(f"\n[red]Error: Path '{db_path}' is a directory[/]")
            if not Confirm.ask("\n[yellow]Try different path?[/]"): console.print("[red]Cancelled[/]"); sys.exit(0)
            continue
        try:
            try: uri_path = f"file:{db_path.resolve()}?mode=ro"; conn_check = sqlite3.connect(uri_path, uri=True)
            except sqlite3.OperationalError: conn_check = sqlite3.connect(db_path)
            with conn_check as conn:
                cursor = conn.cursor(); cursor.execute("SELECT sqlite_version()"); version = cursor.fetchone()[0]
                console.print(f"\n[green]✓ Valid SQLite DB (v{version})[/]"); return db_path
        except sqlite3.Error as e:
            console.print(f"\n[red]Error: Not valid SQLite: {e}[/]")
            if not Confirm.ask("\n[yellow]Try different path?[/]"): console.print("[red]Cancelled[/]"); sys.exit(0)
        except Exception as e:
             console.print(f"\n[red]Unexpected error opening SQLite: {e}[/]")
             if not Confirm.ask("\n[yellow]Try different path?[/]"): console.print("[red]Cancelled[/]"); sys.exit(0)

def test_pg_connection(config: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Test PostgreSQL connection"""
    try:
        conn_info = psycopg.conninfo.make_conninfo(**config)
        with psycopg.connect(conn_info, connect_timeout=5) as conn:
            with conn.cursor() as cur: cur.execute("SELECT 1")
        return True, None
    except psycopg.OperationalError as e:
        error_msg = str(e).strip()
        if "authentication failed" in error_msg.lower(): return False, f"Auth failed for user '{config.get('user', 'N/A')}'."
        elif "database" in error_msg.lower() and "does not exist" in error_msg.lower(): return False, f"DB '{config.get('dbname', 'N/A')}' does not exist."
        elif "connection refused" in error_msg.lower() or "could not connect" in error_msg.lower(): return False, f"Connection refused at {config.get('host','N/A')}:{config.get('port','N/A')}."
        elif "role" in error_msg.lower() and "does not exist" in error_msg.lower(): return False, f"User (role) '{config.get('user', 'N/A')}' does not exist."
        else: return False, f"DB connection error: {error_msg}"
    except Exception as e: return False, f"Unexpected connection test error: {e}"

def get_pg_config() -> Dict[str, Any]:
    """Interactive config for PostgreSQL connection"""
    while True:
        console.print(Panel("PostgreSQL Connection Configuration", style="cyan"))
        config = {}; defaults = {'host': 'localhost', 'port': 5432, 'dbname': 'postgres', 'user': 'postgres', 'password': ''}
        config['host'] = Prompt.ask("[cyan]PG host[/]", default=defaults['host'])
        config['port'] = IntPrompt.ask("[cyan]PG port[/]", default=defaults['port'])
        config['dbname'] = Prompt.ask("[cyan]Database name[/]", default=defaults['dbname'])
        config['user'] = Prompt.ask("[cyan]Username[/]", default=defaults['user'])
        config['password'] = Prompt.ask("[cyan]Password[/]", password=True)
        summary = Table(show_header=False, box=None)
        summary.add_row("[cyan]Host:[/]", config['host']); summary.add_row("[cyan]Port:[/]", str(config['port']))
        summary.add_row("[cyan]Database:[/]", config['dbname']); summary.add_row("[cyan]User:[/]", config['user'])
        summary.add_row("[cyan]Password:[/]", "********")
        console.print("\nConnection Details:"); console.print(summary)
        with console.status("[cyan]Testing DB connection...[/]", spinner="dots") as status:
            success, error_msg = test_pg_connection(config)
            if success: status.update("[green]✓ DB connection successful![/]")
            else: status.stop(); console.print(f"\n[red]Connection Error: {error_msg}[/]")
        if not success:
            if not Confirm.ask("\n[yellow]Try again?[/]"): console.print("[red]Cancelled[/]"); sys.exit(0)
            console.print("\n"); continue
        if Confirm.ask("\n[yellow]Proceed?[/]"): return config
        else:
            if not Confirm.ask("[yellow]Enter different settings?[/]"): console.print("[red]Cancelled[/]"); sys.exit(0)
            console.print("\n")

def get_batch_config() -> int:
    """Interactive config for batch size"""
    console.print(Panel("Batch Size Configuration", style="cyan"))
    console.print("[cyan]Batch size (records/memory usage). Recommended: 100-5000[/]\n")
    while True:
        batch_size = IntPrompt.ask("[cyan]Batch size[/]", default=500)
        if batch_size < 1: console.print("[red]Min 1[/]"); continue
        if batch_size > 10000:
            if not Confirm.ask(f"[yellow]Batch size {batch_size} is large. Continue?[/]"): continue
        return batch_size

# --- Integrity Check and Type Mapping ---
def check_sqlite_integrity(db_path: Path) -> bool:
    """Run integrity check on SQLite database"""
    console.print(Panel("Running SQLite Database Integrity Check", style="cyan"))
    try: db_uri = f'file:{db_path.resolve()}?mode=ro'; conn = sqlite3.connect(db_uri, uri=True, timeout=10)
    except sqlite3.OperationalError:
        console.print("[yellow]Read-only URI mode failed, connecting normally.[/]")
        try: conn = sqlite3.connect(db_path, timeout=10)
        except sqlite3.Error as e: console.print(f"[bold red]Error connecting for integrity check:[/] {e}"); return False
    try:
        with conn:
            cursor = conn.cursor(); checks = [("Integrity Check", "PRAGMA integrity_check"), ("Foreign Key Check", "PRAGMA foreign_key_check")]
            table = Table(show_header=True, title="Integrity Check Results", box=None)
            table.add_column("Check", style="cyan", justify="right"); table.add_column("Result", style="white"); table.add_column("Status", style="green")
            all_passed = True
            for check_name, query in checks:
                console.print(f"[cyan]Running {check_name}...[/]");
                try:
                    cursor.execute(query); result = cursor.fetchall(); is_ok = (result == [('ok',)] or not result)
                    status_icon, status_text = ("✅", "Passed") if is_ok else ("❌", "Failed")
                    result_str = "ok" if is_ok else str(result); table.add_row(check_name, result_str, f"{status_icon} {status_text}")
                    if not is_ok: all_passed = False; console.print(f"[red]Failed {check_name}:[/] {result}")
                except sqlite3.Error as e: console.print(f"[bold red]Error during '{check_name}': {e}"); table.add_row(check_name, f"Error: {e}", "❌ Failed"); all_passed = False
            try: cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table';"); cursor.fetchone()
            except sqlite3.DatabaseError as e: console.print(f"[bold red]Critical: Cannot query sqlite_master:[/] {e}"); table.add_row("Schema Query", f"Error: {e}", "❌ Failed"); all_passed = False
            console.print(table);
            if not all_passed: console.print("[bold yellow]Integrity checks failed.[/]")
            return all_passed
    except Exception as e: console.print(f"[bold red]Unexpected integrity check error:[/] {e}"); return False
    finally:
        if conn: conn.close()

def sqlite_to_pg_type(sqlite_type: str) -> str:
    """Maps SQLite data types to PostgreSQL data types."""
    sqlite_type_norm = sqlite_type.upper().strip() if sqlite_type else 'TEXT'
    if sqlite_type_norm == 'JSON': return 'TEXT' # Map SQLite 'JSON' name to TEXT for schema creation/comparison
    if 'INT' in sqlite_type_norm: return 'BIGINT' if 'BIGINT' in sqlite_type_norm else 'INTEGER'
    elif 'CHAR' in sqlite_type_norm or 'TEXT' in sqlite_type_norm or 'CLOB' in sqlite_type_norm: return 'TEXT'
    elif 'BLOB' in sqlite_type_norm: return 'BYTEA'
    elif 'REAL' in sqlite_type_norm or 'FLOA' in sqlite_type_norm or 'DOUB' in sqlite_type_norm: return 'DOUBLE PRECISION'
    elif 'NUMERIC' in sqlite_type_norm or 'DECIMAL' in sqlite_type_norm: return 'NUMERIC'
    elif 'BOOL' in sqlite_type_norm: return 'BOOLEAN'
    elif 'DATE' in sqlite_type_norm or 'TIME' in sqlite_type_norm:
         if 'TIMESTAMP' in sqlite_type_norm: return 'TIMESTAMP WITHOUT TIME ZONE'
         elif 'DATE' in sqlite_type_norm: return 'DATE'
         elif 'TIME' in sqlite_type_norm: return 'TIME WITHOUT TIME ZONE'
         return 'TEXT'
    else: console.print(f"[yellow]Warning: Unknown SQLite type '{sqlite_type}'. Mapping to TEXT.[/]"); return 'TEXT'

def get_sqlite_safe_identifier(identifier: str) -> str: return f'"{identifier}"'

def get_pg_safe_identifier(identifier: str) -> str:
    """Quotes identifiers for PostgreSQL if necessary."""
    lower_id = identifier.lower(); reserved_keywords = { 'all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc', 'asymmetric', 'authorization', 'between', 'binary', 'both', 'case', 'cast', 'check', 'collate', 'column', 'concurrently', 'constraint', 'create', 'cross', 'current_catalog', 'current_date', 'current_role', 'current_schema', 'current_time', 'current_timestamp', 'current_user', 'default', 'deferrable', 'desc', 'distinct', 'do', 'else', 'end', 'except', 'false', 'fetch', 'for', 'foreign', 'freeze', 'from', 'full', 'grant', 'group', 'having', 'ilike', 'in', 'initially', 'inner', 'intersect', 'into', 'is', 'isnull', 'join', 'lateral', 'leading', 'left', 'like', 'limit', 'localtime', 'localtimestamp', 'natural', 'not', 'notnull', 'null', 'offset', 'on', 'only', 'or', 'order', 'outer', 'overlaps', 'placing', 'primary', 'references', 'returning', 'right', 'select', 'session_user', 'similar', 'some', 'symmetric', 'table', 'tablesample', 'then', 'to', 'trailing', 'true', 'union', 'unique', 'user', 'using', 'variadic', 'verbose', 'when', 'where', 'window', 'with'}
    if lower_id in reserved_keywords or not identifier.isidentifier() or identifier != lower_id:
        escaped_identifier = identifier.replace("\"", "\"\"")
        return f'"{escaped_identifier}"'
    return identifier

# --- Async Context Manager ---
@asynccontextmanager
async def async_db_connections(sqlite_path: Path, pg_config: Dict[str, Any]):
    """Manages async connections to SQLite (RO) and PostgreSQL."""
    sqlite_conn: Optional[sqlite3.Connection] = None; pg_conn: Optional[psycopg.AsyncConnection] = None
    try:
        try: # Connect SQLite RO
            db_uri = f'file:{sqlite_path.resolve()}?mode=ro'; console.print(f"[dim]Attempting SQLite connection: {db_uri}[/dim]")
            sqlite_conn = sqlite3.connect(db_uri, uri=True, timeout=60)
            try: sqlite_conn.execute('PRAGMA cache_size=-40000;') # Set cache
            except sqlite3.Error as pragma_e: console.print(f"[yellow]Warning: SQLite cache_size pragma failed: {pragma_e}[/yellow]")
            try: # Verify RO
                 cursor = sqlite_conn.cursor(); cursor.execute("PRAGMA query_only;"); is_read_only = cursor.fetchone()[0]
                 if is_read_only == 1: console.print("[green dim]✓ SQLite connection is read-only.[/green dim]")
                 else: console.print("[yellow]Warning: SQLite connection NOT read-only.[/yellow]")
                 cursor.close()
            except sqlite3.Error as ro_check_e: console.print(f"[yellow]Warning: SQLite RO check failed: {ro_check_e}[/yellow]")
            sqlite_conn.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            console.print(f"[bold red]SQLite connection failed:[/] {e}"); console.print(f"[bold red]Path:[/] {sqlite_path.resolve()}")
            if "disk I/O error" in str(e).lower(): console.print("[bold yellow]Suggestion: Check file lock, permissions, disk space.[/]")
            raise
        try: # Connect PostgreSQL
            conn_info = psycopg.conninfo.make_conninfo(**pg_config)
            pg_conn = await psycopg.AsyncConnection.connect(conn_info, autocommit=False)
        except psycopg.Error as e: console.print(f"[bold red]PG connection failed:[/] {e}"); raise
        except Exception as e: console.print(f"[bold red]Unexpected PG connect error:[/] {e}"); raise
        yield sqlite_conn, pg_conn # Yield connections
    finally: # Close connections
        if sqlite_conn:
            try: sqlite_conn.close()
            except Exception as e: console.print(f"[yellow]Warning: SQLite close error: {e}[/]")
        if pg_conn and not pg_conn.closed:
            try: await pg_conn.close()
            except Exception as e: console.print(f"[yellow]Warning: PG close error: {e}[/]")

# --- Data Cleaning and Processing ---

async def get_pg_column_types(pg_cursor: psycopg.AsyncCursor, table_name: str) -> Dict[str, str]:
    """Fetches column names and data types for a PG table."""
    types = {}
    try:
        await pg_cursor.execute("SELECT lower(column_name), lower(data_type) FROM information_schema.columns WHERE lower(table_name) = lower(%s)", (table_name,))
        types = {row[0]: row[1] for row in await pg_cursor.fetchall()}
        await pg_cursor.connection.commit() # Commit read transaction
    except psycopg.Error as e: console.print(f"[yellow]Warning: Fetch PG types failed {table_name}: {e}"); await pg_cursor.connection.rollback()
    except Exception as e: console.print(f"[red]Unexpected fetch PG types error {table_name}: {e}"); await pg_cursor.connection.rollback()
    return types

# *** REVISED clean_value WITH BOOLEAN HANDLING ***
def clean_value(value: Any, target_pg_type: Optional[str], column_name: str) -> Any:
    """Clean value for PostgreSQL insertion: handle nulls, JSON, booleans, null bytes."""
    if value is None: return None
    target_pg_type_lower = target_pg_type.lower() if target_pg_type else None

    # Handle SQLite INTEGER (0/1) mapped to PostgreSQL BOOLEAN
    if target_pg_type_lower == 'boolean' and isinstance(value, int):
        if value == 0: return False
        elif value == 1: return True
        else:
            console.print(f"[yellow]Warning: Col '{column_name}' expects boolean, received int '{value}'. Mapping to NULL.[/yellow]")
            return None # Map unexpected integers to NULL

    # Handle potential JSON stored as TEXT in SQLite
    elif target_pg_type_lower in ('json', 'jsonb') and isinstance(value, str):
        try:
            parsed_json = json.loads(value); return Json(parsed_json) # Use Json adapter
        except json.JSONDecodeError:
            if value.strip() == '': return None # Map empty string for JSON col to NULL
            else:
                console.print(f"[yellow]Warning: Col '{column_name}' expects {target_pg_type}, invalid JSON. Mapping NULL. Preview: '{str(value)[:50]}...'[/]")
                return None # Map invalid JSON to NULL
        except Exception as e:
             console.print(f"[red]Error processing JSON col '{column_name}': {e}. Mapping NULL. Preview: '{str(value)[:50]}...'[/]")
             return None # Map other JSON errors to NULL

    # Clean null bytes only from strings for other types
    elif isinstance(value, str): return value.replace('\x00', '')

    # Let psycopg handle bytes, float, bool (if already bool), standard int, etc.
    return value

async def process_table(
    table_name: str, sqlite_conn: sqlite3.Connection, pg_conn: psycopg.AsyncConnection,
    progress: Progress, batch_size: int
) -> bool:
    """Processes a single table from SQLite to PostgreSQL."""
    pg_safe_table_name = get_pg_safe_identifier(table_name)
    sqlite_safe_table_name = get_sqlite_safe_identifier(table_name)
    total_rows, processed_rows, task_id, table_failed = 0, 0, None, False

    sqlite_schema_cursor = sqlite_conn.cursor(); sqlite_data_cursor = sqlite_conn.cursor(); pg_cursor = pg_conn.cursor()
    console.print(f"\n[cyan]Processing table: {table_name}[/]")

    try: # Get SQLite Schema & Count
        try:
            sqlite_schema_cursor.execute(f'PRAGMA table_info({sqlite_safe_table_name})'); schema_tuples = sqlite_schema_cursor.fetchall()
            if not schema_tuples: console.print(f"[yellow]Skipping '{table_name}': No schema.[/]"); return True
            sqlite_schema_cursor.execute(f"SELECT COUNT(*) FROM {sqlite_safe_table_name}"); count_result = sqlite_schema_cursor.fetchone(); total_rows = count_result[0] if count_result else 0
        except sqlite3.Error as e: console.print(f"[red]SQLite schema/count error {table_name}: {e}[/]"); return False
        if total_rows == 0: console.print(f"[green]Table '{table_name}' empty.[/]")
        else: console.print(f"[cyan]Found {total_rows} rows in '{table_name}'.[/]")

        # Prepare PG Table
        pg_col_defs, pg_col_names, sqlite_col_names, sqlite_col_names_raw = [], [], [], []
        for col_info in schema_tuples:
            col_name = col_info[1]; sqlite_col_names_raw.append(col_name); sqlite_col_names.append(get_sqlite_safe_identifier(col_name))
            pg_safe_name = get_pg_safe_identifier(col_name); pg_col_names.append(pg_safe_name)
            pg_type = sqlite_to_pg_type(col_info[2]); pg_col_defs.append(f"{pg_safe_name} {pg_type}")

        pg_column_types = await get_pg_column_types(pg_cursor, table_name)
        if not pg_column_types: console.print(f"[yellow]Could not get PG schema for {table_name}.[/]")

        # Truncate PG Table
        try:
            console.print(f"[cyan]Truncating {pg_safe_table_name}...[/]")
            await pg_cursor.execute(f"TRUNCATE TABLE {pg_safe_table_name} RESTART IDENTITY CASCADE"); await pg_conn.commit()
        except psycopg.Error as e: console.print(f"[yellow]Warning: Truncate {pg_safe_table_name} failed: {e}.[/]"); await pg_conn.rollback()

        # Migrate Data
        if total_rows > 0:
            task_id = progress.add_task(f"Migrating {table_name}", total=total_rows)
            placeholders = ', '.join(['%s'] * len(pg_col_names))
            insert_sql = f"INSERT INTO {pg_safe_table_name} ({', '.join(pg_col_names)}) VALUES ({placeholders})"
            sqlite_query = f"SELECT {', '.join(sqlite_col_names)} FROM {sqlite_safe_table_name}"
            sqlite_data_cursor.execute(sqlite_query)

            while True:
                batch_data_raw = sqlite_data_cursor.fetchmany(batch_size);
                if not batch_data_raw: break
                batch_data_cleaned = []
                try: # Clean batch
                    for row_raw in batch_data_raw:
                         cleaned_row = [clean_value(cell, pg_column_types.get(sqlite_col_names_raw[i].lower()), sqlite_col_names_raw[i]) for i, cell in enumerate(row_raw)]
                         batch_data_cleaned.append(tuple(cleaned_row))
                except Exception as e:
                     console.print(f"[red]Batch cleaning error {table_name}: {e}[/]")
                     progress.console.print(f"[yellow]Skipping batch ({len(batch_data_raw)} rows) {table_name} cleaning error.[/]")
                     processed_rows += len(batch_data_raw);
                     if task_id is not None: progress.update(task_id, advance=len(batch_data_raw)); continue

                try: # Insert batch
                    await pg_cursor.executemany(insert_sql, batch_data_cleaned); await pg_conn.commit()
                    processed_rows += len(batch_data_cleaned)
                    if task_id is not None: progress.update(task_id, advance=len(batch_data_cleaned))
                except psycopg.Error as batch_e: # Batch failed, try row-by-row
                    await pg_conn.rollback(); progress.console.print(f"\n[yellow]Batch insert failed {table_name}. Row-by-row...[/]")
                    progress.console.print(f"[yellow dim]Batch Error ({type(batch_e).__name__}): {batch_e}[/]")
                    failed_rows, current_idx = 0, processed_rows
                    for i, row_cleaned in enumerate(batch_data_cleaned):
                        current_idx += 1
                        try: await pg_cursor.execute(insert_sql, row_cleaned); await pg_conn.commit()
                        except psycopg.Error as row_e:
                            await pg_conn.rollback(); failed_rows += 1
                            progress.console.print(f"[red]--> Failed row {current_idx} {table_name}:[/]")
                            progress.console.print(f"[red dim]  ErrType: {type(row_e).__name__}[/]")
                            if hasattr(row_e, 'diag') and row_e.diag:
                                progress.console.print(f"[red dim]  PG Code: {row_e.diag.sqlstate or 'N/A'} | Msg: {row_e.diag.message_primary or 'N/A'}[/]")
                                if row_e.diag.message_detail: progress.console.print(f"[red dim]  Detail: {row_e.diag.message_detail}[/]")
                                if row_e.diag.message_hint: progress.console.print(f"[red dim]  Hint: {row_e.diag.message_hint}[/]")
                                if row_e.diag.column_name: progress.console.print(f"[red dim]  Column: {row_e.diag.column_name}[/]")
                                if row_e.diag.constraint_name: progress.console.print(f"[red dim]  Constr: {row_e.diag.constraint_name}[/]")
                            else: progress.console.print(f"[red dim]  Raw Err: {row_e}[/]")
                            try: preview = {sqlite_col_names_raw[j]: str(val)[:50] + ('...' if len(str(val)) > 50 else '') for j, val in enumerate(row_cleaned)}; progress.console.print(f"[red dim]  Preview: {preview}[/]")
                            except Exception: progress.console.print("[red dim]  (Preview failed)[/]")
                        except Exception as row_e_unexp: await pg_conn.rollback(); failed_rows += 1; progress.console.print(f"[red]--> Unexp. row error {current_idx} {table_name}: {row_e_unexp}[/]")
                    if task_id is not None: progress.update(task_id, advance=len(batch_data_cleaned)) # Advance past batch
                    processed_rows += len(batch_data_cleaned) # Increment count
                    if failed_rows > 0: table_failed = True; console.print(f"[yellow]{failed_rows}/{len(batch_data_cleaned)} rows failed {table_name}.[/]")
                except Exception as e_unexp: await pg_conn.rollback(); console.print(f"\n[bold red]Unexpected batch error {table_name}: {e_unexp}[/]"); console.print(traceback.format_exc()); return False

        # Finalize table status
        final_status = "[green]✓ Migrated" if not table_failed else "[yellow]⚠️ Partial/Failed"
        if task_id is not None: progress.update(task_id, completed=total_rows, description=f"{final_status} {table_name}")
        else:
             if total_rows == 0: console.print(f"[green]✓ Ensured empty table: {table_name}[/]")
        console.print(f"{final_status} table: {table_name}. Processed {processed_rows}/{total_rows} rows attempt.")
        return not table_failed

    except Exception as e: # Catch errors during table processing setup
        console.print(f"[bold red]Critical table processing error {table_name}: {e}[/]"); console.print(traceback.format_exc())
        try:
             if not pg_conn.closed and pg_conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE: await pg_conn.rollback()
        except Exception as rb_e: console.print(f"[yellow]Warning: Final rollback error: {rb_e}[/]")
        if task_id is not None: progress.update(task_id, description=f"[red]Failed: {table_name}", completed=total_rows)
        return False
    finally: # Close cursors for this table
        if pg_cursor and not pg_cursor.closed: await pg_cursor.close()
        if sqlite_schema_cursor: sqlite_schema_cursor.close();
        if sqlite_data_cursor: sqlite_data_cursor.close()

# --- Main Migration Logic ---
async def migrate() -> None:
    """Main migration function."""
    console.print(Panel("SQLite to PostgreSQL Migration Tool", style="bold blue", subtitle="v4 - Boolean Fix"))
    sqlite_path = get_sqlite_config()
    if not check_sqlite_integrity(sqlite_path):
        if not Confirm.ask("[bold yellow]DB integrity issues. Continue anyway?", default=False): console.print("[red]Aborted.[/]"); sys.exit(1)
        else: console.print("[yellow]Proceeding despite warnings...[/]")
    pg_config = get_pg_config(); batch_size = get_batch_config()
    console.print(Panel("Starting Migration Process", style="cyan", border_style="yellow")); start_time = asyncio.get_event_loop().time()
    processed_table_count, failed_tables_list = 0, []
    skipped_tables = ["migratehistory", "alembic_version", "sqlite_sequence"]

    try:
        async with async_db_connections(sqlite_path, pg_config) as (sqlite_conn, pg_conn):
            if pg_conn.closed: raise psycopg.OperationalError("PG connection closed unexpectedly.")
            console.print("[green]✓ DB connections established.[/]")
            sqlite_cursor = sqlite_conn.cursor(); sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables_to_migrate = [row[0] for row in sqlite_cursor.fetchall()]; sqlite_cursor.close()
            if not tables_to_migrate: console.print("[yellow]No user tables found.[/]"); sys.exit(0)
            console.print(f"[cyan]Found {len(tables_to_migrate)} user tables.[/]")

            with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.1f}%"), TextColumn("[cyan]{task.completed}/{task.total} rows"), TimeElapsedColumn(), console=console, transient=False) as progress:
                for table_name in tables_to_migrate:
                    if table_name in skipped_tables: console.print(f"\n[yellow dim]Skipping: {table_name}[/]"); continue
                    success = await process_table(table_name, sqlite_conn, pg_conn, progress, batch_size)
                    if success: processed_table_count += 1
                    else: failed_tables_list.append(table_name)
    except sqlite3.Error as e: console.print(f"\n[bold red]Fatal SQLite Error: {e}[/]"); console.print(traceback.format_exc()); sys.exit(1)
    except psycopg.Error as e: console.print(f"\n[bold red]Fatal PostgreSQL Error: {e}[/]"); console.print(traceback.format_exc()); sys.exit(1)
    except Exception as e: console.print(f"\n[bold red]Critical unexpected migration error: {e}[/]"); console.print(traceback.format_exc()); sys.exit(1)

    # --- Final Summary ---
    end_time = asyncio.get_event_loop().time(); duration = end_time - start_time
    console.print("\n" + "="*60)
    if not failed_tables_list:
        console.print(Panel(f"🎉 Migration Complete! 🎉\n\nSuccessfully processed {processed_table_count} tables.\nDuration: {duration:.2f} seconds.", style="bold green", title="Success"))
    else:
        console.print(Panel(f"⚠️ Migration Completed with Errors ⚠️\n\nSucceeded: {processed_table_count} tables.\nFailed: {len(failed_tables_list)} tables.\nDuration: {duration:.2f} seconds.", style="bold yellow", title="Partial Success / Errors"))
        console.print("\n[bold red]Failed tables:[/]"); [console.print(f"- {table}") for table in failed_tables_list]
        console.print("\n[yellow]Review logs for specific row errors.[/]")
        log_filename = f"migration_log_{Path(sqlite_path).stem}_{start_time:.0f}.txt"
        try: console.save_text(log_filename); console.print(f"\n[cyan]Full log saved to: {log_filename}[/]")
        except Exception as e_log: console.print(f"[red]Could not save log: {e_log}[/]")

if __name__ == "__main__":
    if sys.platform == "win32":
        try: asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception as e: console.print(f"[yellow]Warning: Could not set Win event loop policy: {e}[/yellow]")
    try: asyncio.run(migrate())
    except KeyboardInterrupt: console.print("\n[bold yellow]Interrupted.[/]"); sys.exit(1)
    except SystemExit as e:
         if e.code != 0: pass # Non-zero exit, potentially log
         raise
    except Exception as e:
        console.print(f"\n[bold red]Unexpected top-level error:[/]"); console.print(traceback.format_exc()); sys.exit(1)