# -*- coding: utf-8 -*-
import psycopg
import sys
import traceback
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from typing import Dict, Any, Set, Tuple, Optional, List

console = Console()

# --- !! IMPORTANT: VERIFY TABLE/COLUMN NAMES !! ---
# Use Adminer or psql to check the exact names in your DB.
DB_CHAT_TABLE_NAME = "chat"
DB_CHAT_ID_COLUMN = "id"
DB_CHAT_SHARE_ID_COLUMN = "share_id"
DB_CHAT_TITLE_COLUMN = "title" # Assumed column name for context
# -----------------------------------------------

# --- Database Connection Functions (Reused) ---
def test_pg_connection(config: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Test PostgreSQL connection"""
    try:
        conn_info = psycopg.conninfo.make_conninfo(**config)
        with psycopg.connect(conn_info, connect_timeout=5) as conn:
            with conn.cursor() as cur: cur.execute("SELECT 1")
        return True, None
    except psycopg.OperationalError as e:
        error_msg = str(e).strip()
        # Simplified error messages for brevity
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

# --- Core Script Functions ---

def fetch_shared_chats(conn: psycopg.Connection) -> Optional[List[Dict[str, Any]]]:
    """Fetches chats with non-NULL share_id."""
    table_name = DB_CHAT_TABLE_NAME
    id_col = DB_CHAT_ID_COLUMN
    share_id_col = DB_CHAT_SHARE_ID_COLUMN
    title_col = DB_CHAT_TITLE_COLUMN # Use configured title column

    console.print(f"[cyan]Fetching shared chats from DB table '{table_name}'...[/]")
    shared_chats: List[Dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            # Use safe quoting for identifiers IF necessary (simple check)
            safe_table = f'"{table_name}"' if not table_name.isidentifier() else table_name
            safe_id = f'"{id_col}"' if not id_col.isidentifier() else id_col
            safe_share_id = f'"{share_id_col}"' if not share_id_col.isidentifier() else share_id_col
            safe_title = f'"{title_col}"' if not title_col.isidentifier() else title_col

            # Construct the query safely
            query = f"""
                SELECT {safe_id}, {safe_share_id}, {safe_title}
                FROM {safe_table}
                WHERE {safe_share_id} IS NOT NULL
                ORDER BY {safe_title};
            """
            # console.print(f"[DEBUG] Executing: {query}") # Uncomment for debug
            cur.execute(query)
            results = cur.fetchall()

            # Check if title column exists by inspecting cursor description if needed
            col_names = [desc[0] for desc in cur.description] if cur.description else []
            has_title_col = title_col in col_names

            for row in results:
                chat_data = {
                    'id': str(row[0]), # Ensure ID is string
                    'share_id': str(row[1]), # Ensure share ID is string
                    'title': str(row[2]) if has_title_col and len(row) > 2 and row[2] else "[No Title]"
                }
                shared_chats.append(chat_data)

        console.print(f"[green]✓ Found {len(shared_chats)} chat(s) with active share links.[/]")
        return shared_chats

    except psycopg.errors.UndefinedTable:
         console.print(f"[bold red]DB Error: Table '{table_name}' does not exist![/]")
         console.print(f"[yellow]=> Verify DB_CHAT_TABLE_NAME ('{table_name}') at the top of the script.[/]")
         return None
    except psycopg.errors.UndefinedColumn as e:
         console.print(f"[bold red]DB Error: A required column does not exist: {e}[/]")
         console.print(f"[yellow]=> Verify column names (DB_CHAT_ID_COLUMN, DB_CHAT_SHARE_ID_COLUMN, DB_CHAT_TITLE_COLUMN) at the top.[/]")
         return None
    except psycopg.Error as e:
        console.print(f"[red]DB Error: Failed to fetch shared chats from table '{table_name}'.[/]")
        console.print_exception()
        return None

def display_chats_for_selection(chats: List[Dict[str, Any]]):
    """Displays the list of shared chats in a table for user selection."""
    if not chats:
        return

    table = Table(title="Chats with Active Share Links", show_lines=True, expand=True)
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Chat Title", style="cyan", no_wrap=False) # Allow wrapping for long titles
    table.add_column("Chat ID", style="magenta", no_wrap=True)
    table.add_column("Share ID", style="green", no_wrap=True)

    for idx, chat in enumerate(chats):
        table.add_row(
            str(idx + 1),
            chat['title'],
            chat['id'],
            chat['share_id']
        )
    console.print(table)

def get_user_selection(num_chats: int) -> Optional[List[int]]:
    """Prompts user to select chats by number and validates input."""
    if num_chats == 0:
        return None

    selected_indices: List[int] = []
    while True:
        raw_input = Prompt.ask(
            f"\n[cyan]Enter the number(s) of the chat(s) to revoke share links for[/]\n"
            f"[dim](e.g., '1' or '1, 3, 5', type 'all' to select all, or 'q' to quit)[/]"
        ).strip().lower()

        if raw_input == 'q':
            return None
        if raw_input == 'all':
            return list(range(num_chats)) # Return all indices (0 to num_chats-1)

        parts = raw_input.split(',')
        valid_input = True
        selected_indices_temp: Set[int] = set() # Use set to avoid duplicates

        for part in parts:
            part = part.strip()
            if not part.isdigit():
                console.print(f"[red]Invalid input: '{part}' is not a number.[/]")
                valid_input = False
                break
            try:
                num = int(part)
                if 1 <= num <= num_chats:
                    selected_indices_temp.add(num - 1) # Store 0-based index
                else:
                    console.print(f"[red]Invalid number: {num}. Must be between 1 and {num_chats}.[/]")
                    valid_input = False
                    break
            except ValueError:
                 console.print(f"[red]Invalid input: Could not process '{part}'.[/]")
                 valid_input = False
                 break

        if valid_input:
            selected_indices = sorted(list(selected_indices_temp))
            return selected_indices

        # Loop continues if input was invalid

def revoke_share_links_in_db(conn: psycopg.Connection, chat_ids_to_revoke: List[str]) -> int:
    """Sets share_id to NULL for the specified list of chat IDs."""
    table_name = DB_CHAT_TABLE_NAME
    id_col = DB_CHAT_ID_COLUMN
    share_id_col = DB_CHAT_SHARE_ID_COLUMN

    if not chat_ids_to_revoke:
        return 0

    console.print(f"[yellow]Preparing to revoke {len(chat_ids_to_revoke)} share link(s) in DB table '{table_name}'...[/]")
    revoked_count = -1 # Default to error
    try:
        with conn.cursor() as cur:
            safe_table = f'"{table_name}"' if not table_name.isidentifier() else table_name
            safe_id = f'"{id_col}"' if not id_col.isidentifier() else id_col
            safe_share_id = f'"{share_id_col}"' if not share_id_col.isidentifier() else share_id_col

            update_query = f"UPDATE {safe_table} SET {safe_share_id} = NULL WHERE {safe_id} = ANY(%s);"
            # console.print(f"[DEBUG] Executing: UPDATE {safe_table} SET {safe_share_id} = NULL WHERE {safe_id} = ANY(<{len(chat_ids_to_revoke)} items>);")
            cur.execute(update_query, (chat_ids_to_revoke,)) # Pass list of IDs
            revoked_count = cur.rowcount
            conn.commit()
            console.print(f"[green]✓ Successfully revoked {revoked_count} share link(s).[/]")
            return revoked_count
    except psycopg.Error as e:
        console.print(f"[bold red]DB Error: Failed to revoke share links in '{table_name}'![/]")
        console.print_exception()
        try:
             conn.rollback()
             console.print("[yellow]Transaction rolled back.[/yellow]")
        except Exception as rb_ex:
             console.print(f"[red]Error during rollback: {rb_ex}[/red]")
        return -1 # Indicate failure

# --- Main Execution Logic ---
def run_revoke_process():
    """Executes the main share link revocation process."""
    console.print(Panel("Open WebUI - Chat Share Link Revocation Tool", style="bold blue"))
    pg_db_config = get_pg_config()

    conn: Optional[psycopg.Connection] = None
    try:
        conn_str = psycopg.conninfo.make_conninfo(**pg_db_config)
        with console.status("[cyan]Connecting to PostgreSQL...[/]"):
            conn = psycopg.connect(conn_str)

        # 1. Fetch shared chats
        shared_chats = fetch_shared_chats(conn)

        if shared_chats is None:
            # Error occurred during fetch
            sys.exit(1)
        if not shared_chats:
            console.print("\n[bold green]✓ No chats with active share links found.[/]")
            sys.exit(0)

        # 2. Display chats and get user selection
        display_chats_for_selection(shared_chats)
        selected_indices = get_user_selection(len(shared_chats))

        if selected_indices is None:
            console.print("[cyan]Operation cancelled by user.[/]")
            sys.exit(0)

        if not selected_indices:
            console.print("[yellow]No chats selected.[/]")
            sys.exit(0)

        # 3. Confirm selection
        console.print("\n[bold yellow]You have selected the following chat(s) for share link revocation:[/]")
        selected_chats_to_confirm: List[Dict[str, Any]] = []
        confirmation_table = Table(show_header=False, box=None)
        confirmation_table.add_column("Info", style="cyan")
        confirmation_table.add_column("Value")

        chat_ids_to_revoke: List[str] = []
        for index in selected_indices:
            chat = shared_chats[index]
            selected_chats_to_confirm.append(chat)
            chat_ids_to_revoke.append(chat['id'])
            confirmation_table.add_row(f"  - Title:", f"{chat['title']}")
            confirmation_table.add_row(f"    Chat ID:", f"{chat['id']}")
            confirmation_table.add_row(f"    Share ID:", f"{chat['share_id']}")
            confirmation_table.add_row("","") # Spacer

        console.print(confirmation_table)

        if not Confirm.ask(f"\n[bold red]Permanently revoke the share links for these {len(selected_chats_to_confirm)} chat(s)? This cannot be undone.[/]"):
            console.print("[cyan]Revocation cancelled.[/]")
            sys.exit(0)

        # 4. Perform revocation
        revoked_count = revoke_share_links_in_db(conn, chat_ids_to_revoke)

        if revoked_count < 0:
            console.print("[red]Failed to revoke links due to database error.[/]")
            sys.exit(1)
        elif revoked_count != len(chat_ids_to_revoke):
             console.print(f"[yellow]Warning: Expected to revoke {len(chat_ids_to_revoke)} links, but DB reported {revoked_count} changes.[/]")
        else:
             console.print("[bold green]\nShare link revocation completed successfully.[/]")

    except psycopg.Error as e:
        console.print(f"[bold red]A database error occurred:[/]")
        console.print_exception()
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]An unexpected error occurred:[/]")
        console.print_exception()
        sys.exit(1)
    finally:
        if conn and not conn.closed:
            try: conn.close()
            except Exception: pass # Ignore errors during close

# --- Entry Point ---
if __name__ == "__main__":
    try: run_revoke_process()
    except KeyboardInterrupt: console.print("\n[bold yellow]Operation interrupted.[/]"); sys.exit(1)
    except SystemExit as e: sys.exit(e.code)
    except Exception as e:
        console.print("[bold red]An unexpected critical error occurred:[/]")
        console.print(traceback.format_exc())
        sys.exit(1)