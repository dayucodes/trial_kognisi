import os
import json
import logging
import pandas as pd
import pymysql
import paramiko
import gspread
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sshtunnel import SSHTunnelForwarder
from oauth2client.service_account import ServiceAccountCredentials
from io import StringIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etl")

# ----------------------------
# Snowflake connection
# ----------------------------
def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

def load_to_snowflake(df: pd.DataFrame, table_name: str):
    if df.empty:
        logger.warning("Skipping %s — dataframe is empty", table_name)
        return
    conn = get_snowflake_conn()
    # Uppercase kolom agar sesuai Snowflake convention
    df.columns = [c.upper() for c in df.columns]
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name.upper(), auto_create_table=True, overwrite=True
    )
    logger.info("Loaded %s: %d rows, %d chunks, success=%s", table_name, nrows, nchunks, success)
    conn.close()

# ----------------------------
# Helper: read SQL file
# ----------------------------
def _read_sql_file(path: str) -> str:
    with open(path, "r") as f:
        return f.read()

# ----------------------------
# Helper: MySQL via SSH tunnel
# ----------------------------
def _mysql_query_via_ssh(
    *,
    label: str,
    ssh_host: str,
    ssh_port: int,
    ssh_username: str,
    ssh_private_key_str: str,
    ssh_passphrase: str | None,
    db_host: str,
    db_port: int,
    db_user: str,
    db_password: str,
    db_name: str,
    sql_path: str,
) -> pd.DataFrame:
    try:
        private_key = paramiko.RSAKey.from_private_key(
            StringIO(ssh_private_key_str), password=ssh_passphrase or None
        )
        query = _read_sql_file(sql_path)

        with SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_username,
            ssh_pkey=private_key,
            remote_bind_address=(db_host, db_port),
        ) as tunnel:
            conn = pymysql.connect(
                host="127.0.0.1",
                port=tunnel.local_bind_port,
                user=db_user,
                password=db_password,
                database=db_name,
                cursorclass=pymysql.cursors.DictCursor,
            )
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                df = pd.DataFrame(rows)
                logger.info("[%s] fetched %d rows", label, len(df))
                return df
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
                conn.close()

    except Exception as e:
        logger.exception("[%s] failed: %s", label, e)
        return pd.DataFrame()

# ----------------------------
# Helper: MySQL direct (no SSH)
# ----------------------------
def _mysql_query_direct(
    *,
    label: str,
    db_host: str,
    db_port: int,
    db_user: str,
    db_password: str,
    db_name: str,
    sql_path: str,
) -> pd.DataFrame:
    try:
        query = _read_sql_file(sql_path)
        conn = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            df = pd.DataFrame(rows)
            logger.info("[%s] fetched %d rows", label, len(df))
            return df
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            conn.close()
    except Exception as e:
        logger.exception("[%s] failed: %s", label, e)
        return pd.DataFrame()

# ----------------------------
# Helper: Google Sheets
# ----------------------------
def _gsheet_get_all_records(label: str, spreadsheet_name: str) -> pd.DataFrame:
    try:
        secret_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(secret_info, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(spreadsheet_name)
        sheet = spreadsheet.sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        logger.info("[%s] fetched %d rows", label, len(df))
        return df
    except Exception as e:
        logger.exception("[%s] failed: %s", label, e)
        return pd.DataFrame()

# ----------------------------
# ETL Functions (mirror fetch_data.py)
# ----------------------------

def etl_mykg():
    df = _mysql_query_via_ssh(
        label="MyKG",
        ssh_host=os.environ["SSH_MYKG_HOST"],
        ssh_port=int(os.environ["SSH_MYKG_PORT"]),
        ssh_username=os.environ["SSH_MYKG_USERNAME"],
        ssh_private_key_str=os.environ["KEY_MYKG_ID_RSA_STREAMLIT"],
        ssh_passphrase=os.environ.get("SSH_MYKG_PASSPHRASE"),
        db_host=os.environ["MYKG_HOST"],
        db_port=int(os.environ["MYKG_PORT"]),
        db_user=os.environ["MYKG_USER"],
        db_password=os.environ["MYKG_PASSWORD"],
        db_name=os.environ["MYKG_DATABASE"],
        sql_path="query_mykg.sql",
    )
    load_to_snowflake(df, "MYKG")

def etl_mykg_i():
    df = _mysql_query_via_ssh(
        label="MyKG Instructor",
        ssh_host=os.environ["SSH_MYKG_HOST"],
        ssh_port=int(os.environ["SSH_MYKG_PORT"]),
        ssh_username=os.environ["SSH_MYKG_USERNAME"],
        ssh_private_key_str=os.environ["KEY_MYKG_ID_RSA_STREAMLIT"],
        ssh_passphrase=os.environ.get("SSH_MYKG_PASSPHRASE"),
        db_host=os.environ["MYKG_HOST"],
        db_port=int(os.environ["MYKG_PORT"]),
        db_user=os.environ["MYKG_USER"],
        db_password=os.environ["MYKG_PASSWORD"],
        db_name=os.environ["MYKG_DATABASE"],
        sql_path="query_mykg_i.sql",
    )
    load_to_snowflake(df, "MYKG_INSTRUCTOR")

def etl_self_input():
    df = _mysql_query_via_ssh(
        label="Self Input",
        ssh_host=os.environ["SSH_MYKG_HOST"],
        ssh_port=int(os.environ["SSH_MYKG_PORT"]),
        ssh_username=os.environ["SSH_MYKG_USERNAME"],
        ssh_private_key_str=os.environ["KEY_MYKG_ID_RSA_STREAMLIT"],
        ssh_passphrase=os.environ.get("SSH_MYKG_PASSPHRASE"),
        db_host=os.environ["MYKG_HOST"],
        db_port=int(os.environ["MYKG_PORT"]),
        db_user=os.environ["MYKG_USER"],
        db_password=os.environ["MYKG_PASSWORD"],
        db_name=os.environ["MYKG_DATABASE"],
        sql_path="query_self.sql",
    )
    load_to_snowflake(df, "SELF_INPUT")

def etl_id():
    df = _mysql_query_via_ssh(
        label="Kognisi.id",
        ssh_host=os.environ["SSH_ID_HOST"],
        ssh_port=int(os.environ["SSH_ID_PORT"]),
        ssh_username=os.environ["SSH_ID_USERNAME"],
        ssh_private_key_str=os.environ["KEY_ID_ID_RSA_STREAMLIT"],
        ssh_passphrase=os.environ.get("SSH_ID_PASSPHRASE"),
        db_host=os.environ["ID_HOST"],
        db_port=int(os.environ["ID_PORT"]),
        db_user=os.environ["ID_USER"],
        db_password=os.environ["ID_PASSWORD"],
        db_name=os.environ["ID_DATABASE"],
        sql_path="query_id.sql",
    )
    load_to_snowflake(df, "KOGNISI_ID")

def etl_discovery():
    df = _mysql_query_direct(
        label="Discovery",
        db_host=os.environ["DISCOVERY_HOST"],
        db_port=int(os.environ["DISCOVERY_PORT"]),
        db_user=os.environ["DISCOVERY_USER"],
        db_password=os.environ["DISCOVERY_PASSWORD"],
        db_name=os.environ["DISCOVERY_DATABASE"],
        sql_path="query_discovery.sql",
    )
    load_to_snowflake(df, "DISCOVERY")

def etl_capture():
    df = _gsheet_get_all_records("Capture", "0. Data Capture - Monthly Updated")
    load_to_snowflake(df, "CAPTURE")

def etl_offplatform():
    df = _gsheet_get_all_records("Offplatform", "0. Data Outside Platform")
    load_to_snowflake(df, "OFFPLATFORM")

def etl_clel():
    df = _gsheet_get_all_records("CL EL", "0. Collaborative & Exponential Learners - Monthly Updated")
    load_to_snowflake(df, "CLEL")

def etl_sap():
    df = _gsheet_get_all_records("SAP", "0. Active Employee - Monthly Updated")
    load_to_snowflake(df, "SAP_ACTIVE_EMPLOYEE")

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    logger.info("=== ETL START ===")

    etl_mykg()
    etl_mykg_i()
    etl_self_input()
    etl_id()
    etl_discovery()
    etl_capture()
    etl_offplatform()
    etl_clel()
    etl_sap()

    logger.info("=== ETL DONE ===")
