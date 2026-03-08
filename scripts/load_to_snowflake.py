"""
Load all raw source files into Snowflake RAW schema.
Run from the ICD10/ project root with venv activated.
"""

import os
import csv
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ── Snowflake connection config ────────────────────────────────────────────────
SNOWFLAKE = dict(
    account="",
    user="ADARSH",
    password="",
    role="ACCOUNTADMIN",
    warehouse="COMPUTE_WH",
    database="ICD10_DB",
)

RAW_SCHEMA = "RAW"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")


# ── Helpers ────────────────────────────────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(**SNOWFLAKE)


def setup_schema(cur):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS ICD10_DB.{RAW_SCHEMA}")
    cur.execute(f"USE SCHEMA ICD10_DB.{RAW_SCHEMA}")
    print(f"✓ Schema ICD10_DB.{RAW_SCHEMA} ready")


def load_csv(conn, filepath, table_name, skiprows=0, chunksize=50_000, encoding="utf-8-sig"):
    """Load a CSV file into Snowflake using chunked write_pandas."""
    print(f"\n→ Loading {os.path.basename(filepath)} → {table_name}")
    total = 0
    first_chunk = True

    for chunk in pd.read_csv(
        filepath,
        skiprows=skiprows,
        chunksize=chunksize,
        encoding=encoding,
        low_memory=False,
        dtype=str,          # load everything as string to avoid type issues
        na_filter=False,    # keep empty strings as-is
    ):
        # Clean column names: uppercase, strip, replace spaces/special chars
        chunk.columns = (
            chunk.columns.str.upper()
            .str.strip()
            .str.replace(r"[\n\r\s/\-]+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True)
        )

        success, nchunks, nrows, _ = write_pandas(
            conn,
            chunk,
            table_name=table_name,
            schema=RAW_SCHEMA,
            database="ICD10_DB",
            auto_create_table=first_chunk,
            overwrite=first_chunk,
            quote_identifiers=False,
        )
        total += nrows
        first_chunk = False
        print(f"  ... {total:,} rows loaded", end="\r")

    print(f"  ✓ {table_name}: {total:,} rows loaded")
    return total


def load_icd10_codes(conn, filepath, table_name):
    """ICD-10 code file is fixed-width: first 7 chars = code, rest = description."""
    print(f"\n→ Loading ICD-10 codes → {table_name}")
    rows = []
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if len(line) >= 7:
                code = line[:7].strip()
                description = line[7:].strip()
                if code:
                    rows.append({"ICD10_CODE": code, "DESCRIPTION": description})

    df = pd.DataFrame(rows)
    success, nchunks, nrows, _ = write_pandas(
        conn, df,
        table_name=table_name,
        schema=RAW_SCHEMA,
        database="ICD10_DB",
        auto_create_table=True,
        overwrite=True,
        quote_identifiers=False,
    )
    print(f"  ✓ {table_name}: {nrows:,} rows loaded")


def load_hcc_mappings(conn, filepath, table_name):
    """HCC file has 3 metadata rows before the real header (row index 3)."""
    print(f"\n→ Loading HCC mappings → {table_name}")
    df = pd.read_csv(
        filepath,
        skiprows=3,
        encoding="utf-8-sig",
        dtype=str,
        na_filter=False,
    )
    # Clean multiline column headers
    df.columns = (
        df.columns.str.upper()
        .str.strip()
        .str.replace(r"[\n\r\s/\-]+", "_", regex=True)
        .str.replace(r"[^\w]", "", regex=True)
    )
    success, nchunks, nrows, _ = write_pandas(
        conn, df,
        table_name=table_name,
        schema=RAW_SCHEMA,
        database="ICD10_DB",
        auto_create_table=True,
        overwrite=True,
        quote_identifiers=False,
    )
    print(f"  ✓ {table_name}: {nrows:,} rows loaded")


def load_ccsr(conn, filepath, table_name):
    """CCSR file mixes single-quoted tokens and double-quoted text fields."""
    print(f"\n→ Loading CCSR mappings → {table_name}")
    df = pd.read_csv(
        filepath,
        encoding="utf-8-sig",
        dtype=str,
        na_filter=False,
        quotechar='"',
    )
    # Keep commas inside double-quoted descriptions, then normalize outer single
    # quotes used around many token values.
    df = df.apply(lambda col: col.str.replace(r"^'(.*)'$", r"\1", regex=True))
    df.columns = (
        df.columns.str.upper()
        .str.strip()
        .str.replace(r"[\n\r\s/\-]+", "_", regex=True)
        .str.replace(r"[^\w]", "", regex=True)
    )
    success, nchunks, nrows, _ = write_pandas(
        conn, df,
        table_name=table_name,
        schema=RAW_SCHEMA,
        database="ICD10_DB",
        auto_create_table=True,
        overwrite=True,
        quote_identifiers=False,
    )
    print(f"  ✓ {table_name}: {nrows:,} rows loaded")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("Connecting to Snowflake...")
    conn = get_conn()
    cur = conn.cursor()
    setup_schema(cur)

    raw = DATA_DIR

    # 1. Beneficiary summary
    load_csv(conn, f"{raw}/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv",
             "BENEFICIARY_SUMMARY")

    # 2. Inpatient claims
    load_csv(conn, f"{raw}/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv",
             "CLAIMS_INPATIENT")

    # 3. Outpatient claims
    load_csv(conn, f"{raw}/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv",
             "CLAIMS_OUTPATIENT")

    # 4. Carrier (physician) claims — 1.2GB, chunked
    load_csv(conn, f"{raw}/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv",
             "CLAIMS_CARRIER", chunksize=25_000)

    # 5. ICD-10 code descriptions (fixed-width format)
    load_icd10_codes(conn, f"{raw}/icd10_codes/icd10cm_codes_2024.txt",
                     "ICD10_CODES")

    # 6. CCSR mappings (ICD-10 → clinical category)
    load_ccsr(conn, f"{raw}/DXCCSR_v2026-1.csv", "CCS_MAPPINGS")

    # 7. HCC crosswalk (ICD-10 → HCC category)
    load_hcc_mappings(conn, f"{raw}/2024 Initial ICD-10-CM Mappings.csv",
                      "HCC_CROSSWALK")

    cur.execute(f"USE SCHEMA ICD10_DB.{RAW_SCHEMA}")
    cur.execute("SHOW TABLES")
    tables = cur.fetchall()
    print("\n── Tables in ICD10_DB.RAW ──")
    for t in tables:
        print(f"  {t[1]}")

    conn.close()
    print("\n✓ All done!")


if __name__ == "__main__":
    main()
