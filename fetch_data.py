import streamlit as st
import pandas as pd
import pymysql
from sshtunnel import SSHTunnelForwarder
import paramiko
from io import StringIO
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import logging

# ----------------------------
# Simple profiler (logs to Streamlit logs)
# ----------------------------
logger = logging.getLogger("kognisi_profiler")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

class time_block:
    def __init__(self, name: str):
        self.name = name
        self.t0 = None

    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        ms = (time.perf_counter() - self.t0) * 1000
        logger.info("[PROFILE] %s: %.1fms", self.name, ms)

def _read_sql_file(path: str) -> str:
    with time_block(f"read sql file: {path}"):
        with open(path, "r") as f:
            return f.read()

def _mysql_query_via_ssh(
    *,
    label: str,
    ssh_secret_key: str,
    ssh_secret_cfg: str,
    db_secret_cfg: str,
    sql_path: str,
) -> pd.DataFrame:
    """
    Generic SSH -> MySQL fetcher.
    label: e.g., "mykg", "id", "mykgo"
    ssh_secret_key: st.secrets key containing private key text (e.g., "key_mykg")
    ssh_secret_cfg: st.secrets key containing ssh host/port/username (e.g., "ssh_mykg")
    db_secret_cfg: st.secrets key containing db host/port/user/password/database (e.g., "mykg")
    """
    try:
        with time_block(f"{label}: load private key from secrets"):
            private_key_content = st.secrets[ssh_secret_key]["id_rsa_streamlit"]
            private_key_passphrase = st.secrets[ssh_secret_cfg].get("private_key_passphrase")

        with time_block(f"{label}: parse RSA key"):
            private_key_file = StringIO(private_key_content)
            private_key = paramiko.RSAKey.from_private_key(
                private_key_file, password=private_key_passphrase
            )

        query = _read_sql_file(sql_path)

        with time_block(f"{label}: open SSH tunnel"):
            with SSHTunnelForwarder(
                (st.secrets[ssh_secret_cfg]["host"], st.secrets[ssh_secret_cfg]["port"]),
                ssh_username=st.secrets[ssh_secret_cfg]["username"],
                ssh_pkey=private_key,
                remote_bind_address=(st.secrets[db_secret_cfg]["host"], st.secrets[db_secret_cfg]["port"]),
            ) as tunnel:

                connection_kwargs = {
                    "host": "127.0.0.1",
                    "port": tunnel.local_bind_port if tunnel.is_active else st.secrets[db_secret_cfg]["port"],
                    "user": st.secrets[db_secret_cfg]["user"],
                    "password": st.secrets[db_secret_cfg]["password"],
                    "database": st.secrets[db_secret_cfg]["database"],
                    "cursorclass": pymysql.cursors.DictCursor,
                }

                with time_block(f"{label}: connect mysql"):
                    conn = pymysql.connect(**connection_kwargs)

                try:
                    with time_block(f"{label}: execute sql"):
                        cursor = conn.cursor()
                        cursor.execute(query)

                    with time_block(f"{label}: fetch all rows"):
                        rows = cursor.fetchall()

                    with time_block(f"{label}: build dataframe"):
                        df = pd.DataFrame(rows)

                    return df
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    conn.close()

    except Exception as e:
        st.error(f"An error occurred while fetching data from {label}: {e}")
        logger.exception("[PROFILE] %s failed", label)
        return pd.DataFrame()

def _mysql_query_direct(
    *,
    label: str,
    db_secret_cfg: str,
    sql_path: str,
) -> pd.DataFrame:
    """Direct MySQL fetch (no SSH) e.g., Discovery."""
    try:
        query = _read_sql_file(sql_path)

        connection_kwargs = {
            "host": st.secrets[db_secret_cfg]["host"],
            "port": st.secrets[db_secret_cfg]["port"],
            "user": st.secrets[db_secret_cfg]["user"],
            "password": st.secrets[db_secret_cfg]["password"],
            "database": st.secrets[db_secret_cfg]["database"],
            "cursorclass": pymysql.cursors.DictCursor,
        }

        with time_block(f"{label}: connect mysql"):
            conn = pymysql.connect(**connection_kwargs)

        try:
            with time_block(f"{label}: execute sql"):
                cursor = conn.cursor()
                cursor.execute(query)

            with time_block(f"{label}: fetch all rows"):
                rows = cursor.fetchall()

            with time_block(f"{label}: build dataframe"):
                return pd.DataFrame(rows)
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            conn.close()

    except Exception as e:
        st.error(f"An error occurred while fetching data from {label}: {e}")
        logger.exception("[PROFILE] %s failed", label)
        return pd.DataFrame()

def _gsheet_get_all_records(label: str, spreadsheet_name: str) -> pd.DataFrame:
    try:
        with time_block(f"{label}: authorize gspread"):
            secret_info = st.secrets["json_sap"]
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(secret_info, scope)
            client = gspread.authorize(creds)

        with time_block(f"{label}: open sheet"):
            spreadsheet = client.open(spreadsheet_name)
            sheet = spreadsheet.sheet1

        with time_block(f"{label}: get_all_records"):
            data = sheet.get_all_records()

        with time_block(f"{label}: build dataframe"):
            return pd.DataFrame(data)

    except Exception as e:
        st.error(f"An error occurred while fetching data from {label}: {e}")
        logger.exception("[PROFILE] %s failed", label)
        return pd.DataFrame()

# ----------------------------
# MyKG (SSH)
# ----------------------------
@st.cache_data(ttl=86400, show_spinner=False)
def fetch_data_mykg():
    return _mysql_query_via_ssh(
        label="MyKG",
        ssh_secret_key="key_mykg",
        ssh_secret_cfg="ssh_mykg",
        db_secret_cfg="mykg",
        sql_path="query_mykg.sql",
    )

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_data_mykg_i():
    return _mysql_query_via_ssh(
        label="MyKG Instructor",
        ssh_secret_key="key_mykg",
        ssh_secret_cfg="ssh_mykg",
        db_secret_cfg="mykg",
        sql_path="query_mykg_i.sql",
    )

# ----------------------------
# Self Input (MyKG DB via SSH)
# ----------------------------
@st.cache_data(ttl=86400, show_spinner=False)
def fetch_data_self_input():
    return _mysql_query_via_ssh(
        label="Self Input",
        ssh_secret_key="key_mykg",
        ssh_secret_cfg="ssh_mykg",
        db_secret_cfg="mykg",
        sql_path="query_self.sql",
    )

# ----------------------------
# ID (SSH)
# ----------------------------
@st.cache_data(ttl=86400, show_spinner=False)
def fetch_data_id():
    return _mysql_query_via_ssh(
        label="Kognisi.id",
        ssh_secret_key="key_id",
        ssh_secret_cfg="ssh_id",
        db_secret_cfg="id",
        sql_path="query_id.sql",
    )

# ----------------------------
# Discovery (Direct)
# ----------------------------
@st.cache_data(ttl=86400, show_spinner=False)
def fetch_data_discovery():
    return _mysql_query_direct(
        label="Discovery",
        db_secret_cfg="discovery",
        sql_path="query_discovery.sql",
    )

# ----------------------------
# Google Sheets
# ----------------------------
@st.cache_data(ttl=86400, show_spinner=False)
def fetch_data_capture():
    return _gsheet_get_all_records("Capture (Sheets)", "0. Data Capture - Monthly Updated")

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_data_offplatform():
    return _gsheet_get_all_records("Offplatform (Sheets)", "0. Data Outside Platform")
    
@st.cache_data(ttl=86400, show_spinner=False)
def fetch_data_clel():
    return _gsheet_get_all_records("CL EL (Sheets)", "0. Collaborative & Exponential Learners - Monthly Updated")

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_data_sap_all():
    return _gsheet_get_all_records("SAP Active Employee (Sheets)", "0. Active Employee - Monthly Updated")

def fetch_data_sap(selected_columns):
    df = fetch_data_sap_all()
    existing = [c for c in selected_columns if c in df.columns]
    return df[existing].copy()

# ----------------------------
# MyKGo (SSH)
# ----------------------------
@st.cache_data(ttl=86400, show_spinner=False)
def fetch_data_mykgo():
    return _mysql_query_via_ssh(
        label="MyKGo",
        ssh_secret_key="key_mykgo",
        ssh_secret_cfg="ssh_mykgo",
        db_secret_cfg="mykgo",
        sql_path="query_mykgo.sql",
    )
