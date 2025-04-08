# Open WebUI PostgreSQL Migration & Cleanup Tools

This repository contains a robust and interactive set of Python scripts designed to assist Open WebUI users who are using or migrating to a PostgreSQL database from SQLite.

**Tools Included:**

1.  **`migrate.py`**: Migrates data from an existing Open WebUI SQLite database (`webui.db`) to a PostgreSQL database.
2.  **`clean_stale_models.py`**: Cleans up the `model` table in your PostgreSQL database by removing entries that are no longer listed in the Open WebUI API and are not explicitly marked as disabled (`is_active=False`) in the database. This helps remove orphaned entries after models are deleted or become unavailable.
3.  **`models_check.py`**: A simple utility script to quickly fetch and display the models currently listed by your Open WebUI API endpoint. Useful for debugging or verification.

## Features

*   **Interactive Command-Line Interface**: User-friendly prompts guide you through configuration for database and API connections.
*   **Data Migration**: Transfer user data, chats, settings, etc., from SQLite to PostgreSQL (`migrate.py`).
*   **Automatic Data Type Mapping**: Maps common SQLite data types to appropriate PostgreSQL types during migration (`migrate.py`).
*   **Real-time Progress Visualization**: See the progress of data migration table-by-table with row counts and time elapsed (`migrate.py`).
*   **Stale Model Cleanup**: Identify and remove potentially orphaned model entries from the PostgreSQL `model` table (`clean_stale_models.py`).
*   **Unicode & Special Character Handling**: Designed to handle UTF-8 data commonly found in chat logs and configurations.
*   **Error Handling**: Provides feedback on connection issues, data processing errors, and potential problems.
*   **Batch Processing**: Efficient data transfer during migration using configurable batch sizes (`migrate.py`).
*   **Integrity Checks**: Basic integrity checks on the source SQLite database before migration (`migrate.py`).
*   **Safety Prompts**: Confirmation prompts before performing destructive actions like data truncation or deletion.

## Prerequisites

*   **Python**: Python 3.8 or higher recommended. Python 3.11 is explicitly supported via the `conda` installation option.
*   **pip**: Python package installer.
*   **Open WebUI Instance**: A running instance of Open WebUI.
    *   You'll need its Base URL (e.g., `http://localhost:1337`).
    *   You'll need an API Key (Bearer Token) for the `clean_stale_models.py` script. Generate one in the Open WebUI settings if needed.
*   **Source SQLite Database**: Access to the `webui.db` file (or your named SQLite file) from your *existing* Open WebUI setup.
*   **Target PostgreSQL Database**:
    *   A running PostgreSQL server.
    *   Connection details: Host, Port, Database Name, Username, Password.
    *   **Crucially**: The target PostgreSQL database **must already have the Open WebUI schema created**. See Best Practices below.
*   **(Optional) Conda**: If you prefer using `conda` for environment management, you'll need Anaconda or Miniconda installed.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/open-webui-pg-tools.git # Replace with your repo URL
    cd open-webui-pg-tools
    ```

2.  **Create and activate a virtual environment (Choose ONE method):**

    **Method A: Using `venv` (Standard Python)**
    ```bash
    # Linux/macOS
    python3 -m venv venv
    source venv/bin/activate

    # Windows (cmd/powershell)
    python -m venv venv
    .\venv\Scripts\activate
    ```

    **Method B: Using `conda` (Requires Anaconda/Miniconda)**
    ```bash
    # Create a new conda environment named 'webui-tools' with Python 3.11
    conda create --name webui-tools python=3.11 -y

    # Activate the environment
    conda activate webui-tools
    ```
    *(Note: If you choose `conda`, ensure `pip` is available within the conda environment. It usually is by default.)*

3.  **Install dependencies (inside the activated environment):**
    ```bash
    pip install -r requirements.txt
    ```

## 📝 Best Practices

Following these steps is highly recommended for a smooth migration:

1.  **Before Migration:**
    *   🛑 **BACKUP BOTH YOUR SQLITE AND POSTGRESQL DATABASES!**
    *   **Crucial:** Start Open WebUI at least once with the `DATABASE_URL` environment variable configured to point to your **new, empty PostgreSQL instance**.
        *   This allows Open WebUI to automatically create the necessary tables (bootstrap the schema) in the PostgreSQL database. **The migration script `migrate.py` requires these tables to exist.**
        *   The `DATABASE_URL` format is: `postgresql://user:password@host:port/dbname`
        *   Example (Docker Compose):
            ```yaml
            environment:
              - DATABASE_URL=postgresql://postgres:password@your_pg_host:5432/openwebui_db
            ```
    *   Verify you can connect to the PostgreSQL server from the machine where you will run the migration script. Check firewalls if necessary.
    *   Ensure sufficient disk space on both the source and target systems.

2.  **During Migration (`migrate.py`):**
    *   Avoid interrupting the script while it's running.
    *   Monitor system resources (CPU, RAM, network) if migrating very large databases.
    *   Ensure a stable network connection between the script's host and the PostgreSQL server.

3.  **After Migration (`migrate.py`):**
    *   Stop Open WebUI if it's still running connected to the *old* SQLite database.
    *   Configure Open WebUI to permanently use the **new PostgreSQL database** via the `DATABASE_URL`.
    *   Start Open WebUI connected to PostgreSQL.
    *   Verify data integrity by checking users, chat history, models, etc., within the Open WebUI interface.
    *   Thoroughly test application functionality.

## Usage

⚠️ **IMPORTANT:** Always follow the Best Practices, especially regarding backups and letting Open WebUI create the initial PG schema.

### 1. Migrating Data (`migrate.py`)

This script transfers data from your SQLite `webui.db` to your target PostgreSQL database (which should already have the schema created by Open WebUI).

**Warnings:**

*   This script **TRUNCATES** (deletes all existing data from) the corresponding tables in your PostgreSQL database before inserting data from SQLite. This is why running it on a database *after* Open WebUI has created the schema but *before* significant use is ideal.

**Steps:**

1.  Ensure you have followed the "Before Migration" best practices.
2.  Make sure your virtual environment (`venv` or `conda`) is activated.
3.  Run the script:
    ```bash
    python migrate.py
    ```
4.  Follow the interactive prompts:
    *   Enter the path to your source SQLite database file (e.g., `../open-webui/data/webui.db`).
    *   Enter the connection details for your target PostgreSQL database.
    *   Configure the batch size (default 500 is usually fine). Larger batches can be faster but use more memory.
5.  The script will perform integrity checks, then proceed with migrating data table by table, showing progress.
6.  If errors occur during migration (e.g., data type issues, constraint violations), they will be logged. For critical errors, a log file (`migration_log_*.txt`) might be saved.
7.  Follow the "After Migration" best practices.

### 2. Cleaning Stale Models (`clean_stale_models.py`)

This script identifies and removes entries from the `model` table in PostgreSQL that are considered "stale". Use this *after* you have successfully migrated and are running Open WebUI with PostgreSQL.

**Definition of "Stale":** A model entry in the database is considered stale if it meets **both** of these conditions:

1.  It is **NOT** currently listed in the output of the Open WebUI `/api/models` endpoint.
2.  It is **NOT** explicitly marked as disabled in the database (i.e., its `is_active` column value is not `False` or `0`).

**Warnings:**

*   🛑 **BACKUP YOUR POSTGRESQL DATABASE FIRST!**
*   This script permanently deletes rows from the `model` table.

**Steps:**

1.  Make sure your virtual environment (`venv` or `conda`) is activated.
2.  Run the script:
    ```bash
    python clean_stale_models.py
    ```
3.  Follow the interactive prompts:
    *   Enter your Open WebUI Base URL.
    *   Enter your Open WebUI API Key (Bearer Token).
    *   Enter the connection details for your PostgreSQL database where Open WebUI stores its data.
4.  The script will:
    *   Fetch currently listed models from the API.
    *   Fetch all models and their `is_active` status from the database `model` table.
    *   Identify models present in the database but not listed in the API *and* not marked as inactive.
    *   Display a list of potentially stale models found.
    *   Ask for confirmation before deleting the identified stale entries.

### 3. Checking API Models (`models_check.py`)

A very basic script to quickly query the `/api/models` endpoint and print the results.

**Steps:**

1.  **Edit the script `models_check.py`**: Replace the placeholder values for `api_url` and `api_key` / `headers` with your actual Open WebUI URL and API key.
    ```python
    # In models_check.py
    api_url = "http://your-open-webui-url:port/api/models"
    api_key = "sk-YourActualApiKeyHere..." # Or however your key looks
    headers = {"Authorization": f"Bearer {api_key}"}
    ```
2.  Make sure your virtual environment (`venv` or `conda`) is activated.
3.  Run the script:
    ```bash
    python models_check.py
    ```
4.  The script will print the JSON response from the API, showing the models Open WebUI currently recognizes.

## Safety Features

These scripts include features to help prevent accidental data loss:

*   **`migrate.py`**:
    *   Performs a basic integrity check on the source SQLite database before starting.
    *   Requires explicit confirmation before truncating tables in the target PostgreSQL database.
    *   If a batch insert fails, it attempts to insert rows individually to salvage as much data as possible and logs errors for failed rows.
*   **`clean_stale_models.py`**:
    *   Displays the list of model IDs identified as "stale".
    *   Requires explicit confirmation from the user before proceeding with the deletion from the PostgreSQL database.

## 🚨 Troubleshooting

Common issues and potential solutions:

*   **Connection Errors (PostgreSQL):**
    *   Verify host, port, database name, username, and password are correct in the script prompts.
    *   Check firewall rules on both the client machine (running the script) and the PostgreSQL server.
    *   Ensure the PostgreSQL server is running and accepting connections (`pg_isready` command can help).
    *   Confirm the specified database and user exist and the user has connection privileges (`psql -h HOST -p PORT -U USER -d DBNAME`).
*   **Connection Errors (API - `clean_stale_models.py`):**
    *   Verify the Open WebUI Base URL is correct (e.g., `http://localhost:1337`).
    *   Ensure the API Key (Bearer Token) is valid and hasn't expired or been revoked.
    *   Check network connectivity between the script's machine and the Open WebUI server.
*   **Permission Errors (PostgreSQL):**
    *   The PostgreSQL user needs `SELECT`, `INSERT`, `DELETE`, and `TRUNCATE` privileges on the Open WebUI tables for these scripts to function fully. Grant necessary permissions if needed.
*   **Permission Errors (SQLite):**
    *   Ensure the script has read permissions for the source `webui.db` file and its directory.
*   **`clean_stale_models.py`: `is_active` column not found:** This might indicate you are using a version of Open WebUI that doesn't use this column name or mechanism for disabling models in the database. The script might need adjustment for your specific version.
*   **`migrate.py`: Table not found / Schema mismatch:** This almost always means the target PostgreSQL database **does not have the schema created**. Follow the "Before Migration" best practice: let Open WebUI connect to the PG database *first* to create the tables.
*   **`migrate.py`: Data Type Errors / Row Failures:** Some complex or unusual data in SQLite might cause issues during insertion into PostgreSQL. The script attempts to clean common issues (like null bytes, basic JSON conversion), but specific errors might require manual investigation or adjustments to the `clean_value` function in `migrate.py`. Check the console output and any generated `migration_log_*.txt` files for details on failed rows.
*   **Environment Issues:** Ensure you have activated the correct virtual environment (`venv` or `conda`) before running `pip install` or executing the Python scripts. Check with `which python` (Linux/macOS) or `where python` (Windows).

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs, feature requests, or improvements.

1.  Fork the repository.
2.  Create a new branch (`git checkout -b feature/your-feature-name`).
3.  Make your changes.
4.  Commit your changes (`git commit -am 'Add some feature'`).
5.  Push to the branch (`git push origin feature/your-feature-name`).
6.  Open a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Disclaimer

These scripts are provided "as is" without warranty of any kind. They directly interact with and modify your database. **Always back up your data before use.** The authors are not responsible for any data loss or corruption that may occur as a result of using these tools. Use them at your own risk.