# -*- coding: utf-8 -*-
import requests
import psycopg
import sys
import traceback
import json
from psycopg.types.json import Json, Jsonb
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from typing import Dict, Any, Set, Tuple, Optional, List

console = Console()

# Database table name confirmed as 'model'
DB_MODELS_TABLE_NAME = "model"

# --- Configuration Functions (Assume correct) ---
# [ ... keep get_api_config, test_pg_connection, get_pg_config ... ]
def get_api_config() -> Dict[str, str]:
    """Interactive configuration for Open WebUI API"""
    console.print(Panel("Open WebUI API Configuration", style="cyan"))
    config = {}
    config['url'] = Prompt.ask("[cyan]Open WebUI Base URL[/]", default="http://localhost:1337").rstrip('/')
    config['key'] = Prompt.ask("[cyan]Open WebUI API Key (Bearer Token)[/]", password=True)
    return config

def test_pg_connection(config: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Test PostgreSQL connection"""
    try:
        conn_info = psycopg.conninfo.make_conninfo(**config)
        with psycopg.connect(conn_info, connect_timeout=5) as conn:
            with conn.cursor() as cur: cur.execute("SELECT 1")
        return True, None
    except psycopg.OperationalError as e:
        error_msg = str(e).strip()
        if "authentication failed" in error_msg.lower(): return False, f"Auth failed user '{config.get('user', 'N/A')}'."
        elif "database" in error_msg.lower() and "does not exist" in error_msg.lower(): return False, f"DB '{config.get('dbname', 'N/A')}' not exist."
        elif "connection refused" in error_msg.lower() or "could not connect" in error_msg.lower(): return False, f"Connection refused {config.get('host','N/A')}:{config.get('port','N/A')}."
        elif "role" in error_msg.lower() and "does not exist" in error_msg.lower(): return False, f"User (role) '{config.get('user', 'N/A')}' not exist."
        else: return False, f"DB connection error: {error_msg}"
    except Exception as e: return False, f"Unexpected connection test error: {e}"

def get_pg_config() -> Dict[str, Any]:
    """Interactive config for PostgreSQL connection"""
    while True:
        console.print(Panel("PostgreSQL Connection Configuration", style="cyan"))
        config = {}; defaults = {'host': 'localhost', 'port': 5432, 'dbname': 'openwebui_db', 'user': 'postgres', 'password': ''}
        config['host'] = Prompt.ask("[cyan]PG host[/]", default=defaults['host'])
        config['port'] = Prompt.ask("[cyan]PG port[/]", default=str(defaults['port']))
        config['dbname'] = Prompt.ask("[cyan]Database name[/]", default=defaults['dbname'])
        config['user'] = Prompt.ask("[cyan]Username[/]", default=defaults['user'])
        config['password'] = Prompt.ask("[cyan]Password[/]", password=True)
        try: config['port'] = int(config['port'])
        except ValueError: console.print("[red]Error: Port must be number.[/]"); continue

        summary = Table(show_header=False, box=None)
        summary.add_row("[cyan]Host:[/]", config['host']); summary.add_row("[cyan]Port:[/]", str(config['port']))
        summary.add_row("[cyan]Database:[/]", config['dbname']); summary.add_row("[cyan]User:[/]", config['user'])
        summary.add_row("[cyan]Password:[/]", "********"); console.print("\nDetails:"); console.print(summary)
        with console.status("[cyan]Testing DB connection...[/]", spinner="dots") as status:
            success, error_msg = test_pg_connection(config)
            if success: status.update("[green]✓ DB connection successful![/]")
            else: status.stop(); console.print(f"\n[red]Connection Error: {error_msg}[/]")
        if not success:
            if not Confirm.ask("\n[yellow]Try again?[/]"): console.print("[red]Aborted.[/]"); sys.exit(1)
            console.print("\n"); continue

        if Confirm.ask("\n[yellow]Use these connection settings?[/]"):
             return config
        else:
             if not Confirm.ask("[yellow]Enter different settings?[/]"):
                 console.print("[red]Aborted.[/]"); sys.exit(1)
             console.print("\n")


# --- fetch_api_models (No change needed, it fetches currently active/listed models) ---
def fetch_api_models(api_url: str, api_key: str) -> Optional[Set[str]]:
    """Fetches *currently listed* (usually active) model IDs from the Open WebUI API."""
    target_url = f"{api_url}/api/models"
    headers = {"Authorization": f"Bearer {api_key}"}
    model_ids: Set[str] = set()

    console.print(f"[cyan]Fetching *listed* models from API:[/cyan] {target_url}")
    try:
        with console.status("[cyan]Connecting to API...[/]"):
            response = requests.get(target_url, headers=headers, timeout=30)
            response.raise_for_status()

        data = response.json()
        main_list = None
        if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
            main_list = data['data']
        elif isinstance(data, dict) and 'models' in data and isinstance(data['models'], list):
            main_list = data['models']
        elif isinstance(data, list):
             main_list = data
        else:
            console.print("[red]Error: Could not find main model list (expected root list, or list within 'data' or 'models' key) in API response.[/]")
            if isinstance(data, dict): console.print(f"Keys: {list(data.keys())}")
            return None

        for item in main_list:
            if not isinstance(item, dict): continue # Skip non-dict items

            is_single_model = "id" in item and ("models" not in item or not item.get("models"))
            if is_single_model:
                model_ids.add(str(item["id"]))
            elif "models" in item and isinstance(item["models"], list):
                for nested_model in item["models"]:
                    if isinstance(nested_model, dict) and "id" in nested_model:
                        model_ids.add(str(nested_model["id"]))
            elif "id" in item:
                 model_ids.add(str(item["id"])) # Fallback

        console.print(f"[green]✓ Found {len(model_ids)} unique model IDs listed in API.[/]")
        return model_ids

    except requests.exceptions.RequestException as e: console.print(f"[red]API Error: {e}[/]"); return None
    except json.JSONDecodeError: console.print(f"[red]API JSON Decode Error.[/]"); return None
    except Exception as e: console.print(f"[red]Unexpected API fetch error:[/]"); console.print_exception(); return None

# --- *** REVISED fetch_db_model_states Function *** ---
def fetch_db_model_states(conn: psycopg.Connection) -> Optional[Tuple[Set[str], Set[str]]]:
    """
    Fetches model IDs from the database table.
    Returns a tuple: (all_db_ids, disabled_db_ids)
    """
    table_name = DB_MODELS_TABLE_NAME
    console.print(f"[cyan]Fetching model states from DB table '{table_name}'...[/]")
    all_db_ids: Set[str] = set()
    disabled_db_ids: Set[str] = set()
    try:
        with conn.cursor() as cur:
            safe_table_name = f'"{table_name}"' if not table_name.isidentifier() else table_name
            # Fetch both id and is_active status
            query = f"SELECT id, is_active FROM {safe_table_name};"
            cur.execute(query)
            results = cur.fetchall()

            for row in results:
                if row and len(row) == 2 and row[0] is not None:
                    model_id = str(row[0])
                    is_active_flag = row[1] # This could be True/False (bool) or 1/0 (int)

                    all_db_ids.add(model_id)

                    # Check if inactive (handle both bool False and int 0)
                    if is_active_flag is False or is_active_flag == 0:
                        disabled_db_ids.add(model_id)
                else:
                     console.print(f"[yellow]Warning: Found invalid row in DB '{table_name}', skipping: {row}[/yellow]")

        console.print(f"[green]✓ Found {len(all_db_ids)} total models in DB table '{table_name}'.[/]")
        console.print(f"[green]✓ Found {len(disabled_db_ids)} explicitly disabled models (is_active=False/0) in DB.[/]")
        return all_db_ids, disabled_db_ids
    except psycopg.errors.UndefinedTable:
         console.print(f"[bold red]DB Error: Table '{table_name}' does not exist![/]")
         return None
    except psycopg.errors.UndefinedColumn:
         console.print(f"[bold red]DB Error: Column 'is_active' not found in table '{table_name}'![/]")
         console.print(f"[yellow]=> Does your version of Open WebUI use this column name?[/]")
         return None
    except psycopg.Error as e:
        console.print(f"[red]DB Error: Failed to fetch model states from table '{table_name}'.[/]")
        console.print_exception()
        return None
# --- END REVISED fetch_db_model_states ---

# --- delete_db_models Function (No change needed, uses correct table name) ---
def delete_db_models(conn: psycopg.Connection, ids_to_delete: Tuple[str, ...]) -> int:
    """Deletes models from the specified DB table based on a tuple of IDs."""
    table_name = DB_MODELS_TABLE_NAME
    if not ids_to_delete: return 0

    console.print(f"[yellow]Preparing to delete {len(ids_to_delete)} models from DB table '{table_name}'...[/]")
    deleted_count = -1 # Default to error
    try:
        with conn.cursor() as cur:
            safe_table_name = f'"{table_name}"' if not table_name.isidentifier() else table_name
            delete_query = f"DELETE FROM {safe_table_name} WHERE id = ANY(%s);"
            cur.execute(delete_query, (list(ids_to_delete),))
            deleted_count = cur.rowcount
            conn.commit()
            console.print(f"[green]✓ Successfully deleted {deleted_count} models from '{table_name}'.[/]")
            return deleted_count
    except psycopg.Error as e:
        console.print(f"[bold red]DB Error: Failed to delete models from '{table_name}'![/]")
        console.print_exception()
        try:
             conn.rollback()
             console.print("[yellow]Transaction rolled back.[/yellow]")
        except Exception as rb_ex:
             console.print(f"[red]Error during rollback: {rb_ex}[/red]")
        return -1 # Indicate failure

# --- *** REVISED Main Script Logic *** ---
def run_cleanup():
    """Executes the main cleanup process."""
    console.print(Panel("Open WebUI - Stale Model Cleanup Tool", style="bold blue", subtitle="Use with caution!"))
    api_config = get_api_config()
    pg_db_config = get_pg_config()

    # 1. Fetch models currently listed/active in the API
    api_listed_model_ids = fetch_api_models(api_config['url'], api_config['key'])
    if api_listed_model_ids is None:
        console.print("[red]Aborting due to API error or inability to parse models.[/]")
        sys.exit(1)
    # Note: An empty API list is possible and handled below.

    # 2. Fetch all models and disabled models from the Database
    db_fetch_result: Optional[Tuple[Set[str], Set[str]]] = None
    conn: Optional[psycopg.Connection] = None
    try:
        conn_str = psycopg.conninfo.make_conninfo(**pg_db_config)
        with console.status("[cyan]Connecting to PostgreSQL...[/]"):
            conn = psycopg.connect(conn_str)

        db_fetch_result = fetch_db_model_states(conn)

        if db_fetch_result is None:
            console.print("[red]Aborting due to DB error (table/column not found or query failed).[/]")
            sys.exit(1)

        db_all_model_ids, db_disabled_model_ids = db_fetch_result

        # 3. Determine which models are "safe" to keep
        # Safe = Listed in API OR explicitly disabled in DB
        safe_to_keep_ids = api_listed_model_ids.union(db_disabled_model_ids)
        console.print(f"[cyan]Total models considered 'safe' (in API or disabled in DB): {len(safe_to_keep_ids)}[/]")

        # 4. Identify truly stale models
        # Stale = Exists in DB overall BUT is NOT in the 'safe' list
        stale_ids = db_all_model_ids - safe_to_keep_ids
        stale_ids_list = sorted(list(stale_ids))

        if not stale_ids_list:
            console.print(f"\n[bold green]✓ Database table '{DB_MODELS_TABLE_NAME}' is synchronized. No truly stale models found![/]")
            console.print("[dim](Kept models listed in API and those marked disabled in DB)[/dim]")
            sys.exit(0)

        # 5. Display and confirm deletion
        console.print(f"\n[bold yellow]Found {len(stale_ids_list)} potentially stale model(s) in DB:[/]")
        console.print("[yellow](These models exist in the DB, but are NOT listed in the API AND are NOT marked as disabled in the DB)[/]")

        table = Table(title=f"Stale Model IDs in '{DB_MODELS_TABLE_NAME}' for Deletion", show_lines=True)
        table.add_column("Model ID", style="magenta")
        display_limit = 50
        for i, mid in enumerate(stale_ids_list):
            if i < display_limit:
                 table.add_row(mid)
            elif i == display_limit:
                 table.add_row(f"... and {len(stale_ids_list) - display_limit} more.")
                 break
        console.print(table)

        console.print(f"\n[bold red]WARNING:[/bold red] This will permanently delete these {len(stale_ids_list)} entries from the '{DB_MODELS_TABLE_NAME}' table.")
        if not Confirm.ask(f"[bold yellow]Proceed with deleting these {len(stale_ids_list)} potentially stale models?[/]"):
            console.print("[cyan]Deletion cancelled by user.[/]")
            sys.exit(0)

        # 6. Perform deletion
        deleted_count = delete_db_models(conn, tuple(stale_ids_list))

        if deleted_count < 0:
            console.print("[red]Cleanup failed during deletion process.[/]")
            sys.exit(1)
        elif deleted_count != len(stale_ids_list):
            console.print(f"[yellow]Warning: Script attempted to delete {len(stale_ids_list)} models, but the database reported {deleted_count} deletions.[/]")
        else:
            console.print("[bold green]\nCleanup completed successfully.[/]")

    except psycopg.Error as e:
        console.print(f"[bold red]A database error occurred during connection or cleanup:[/]")
        console.print_exception()
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]An unexpected error occurred:[/]")
        console.print_exception()
        sys.exit(1)
    finally:
        if conn and not conn.closed:
            try: conn.close()
            except Exception as e: console.print(f"[yellow]Warning: Error closing DB connection: {e}[/yellow]")

# --- Main execution block (No changes needed) ---
if __name__ == "__main__":
    try: run_cleanup()
    except KeyboardInterrupt: console.print("\n[bold yellow]Operation interrupted by user.[/]"); sys.exit(1)
    except SystemExit as e:
         if e.code is None or e.code == 0: sys.exit(0)
         else: sys.exit(e.code)
    except Exception as e:
        console.print("[bold red]An unexpected critical error occurred at the top level:[/]")
        console.print_exception(show_locals=False)
        sys.exit(1)