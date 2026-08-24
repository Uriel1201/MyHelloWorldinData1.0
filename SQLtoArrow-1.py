# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "adbc-driver-manager>=1.9.0",
#   "oracledb",
#   "pyarrow",
# ]
# ///
from pathlib import Path

import oracledb as odb
import pyarrow as pa
import pyarrow.dataset as ds
from adbc_driver_manager import dbapi

import config


# ============================================================
# get_query:
# params:
# ============================================================
def get_query(filename: str) -> str:
    try:
        with open(filename, "r", encoding="utf-8") as file:
            return file.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file '{filename}' does not exist.")


# ============================================================
# sqlite_to_arrow:
# params:
# ============================================================
def sqlite_to_arrow(query: str, output_file: str) -> None:
    try:
        with (
            dbapi.connect(
                driver="sqlite",
                db_kwargs={"uri": "file:data/MyDataBase.db?mode=ro"},
            ) as con,
            con.cursor() as cursor,
        ):
            batches = cursor.execute(query).fetch_record_batch()
            with (
                pa.OSFile(f"data/arrow/{output_file}.arrow", "wb") as my_file,
                pa.ipc.new_file(my_file, batches.schema) as writer,
            ):
                for batch in batches:
                    writer.write_batch(batch)
    except Exception as e:
        print(f"DATABASE OPERATION FAILED: {e}")
        raise


# ============================================================
# oracledb_to_arrow:
# params:
# ============================================================
def oracledb_to_arrow(query: str, output_file: str) -> None:
    try:
        with odb.connect(
            user=config.ODB_USER, password=config.ODB_PASSWORD, dsn=config.ODB_DSN
        ) as conn:
            odf = conn.fetch_df_batches(statement=query, size=10000)
            first_df = next(odf)
            batch = pa.RecordBatch.from_arrays(
                first_df.column_arrays(), names=first_df.column_names()
            )
            with (
                pa.OSFile(f"data/arrow/{output_file}.arrow", "wb") as my_file,
                pa.ipc.new_file(my_file, batch.schema) as writer,
            ):
                writer.write(batch)
                for df in odf:
                    batches = pa.RecordBatch.from_arrays(
                                  df.column_arrays(), names=df.column_names()
                    )
                    writer.write_batch(batches)
    except Exception as e:
        print(f"DATABASE OPERATION FAILED: {e}")
        raise


# ============================================================
# arrow_to_mysql:
# params:
# ============================================================
def arrow_to_mysql(arrow_file: str, table_name: str) -> None:

    try:
        with (
            dbapi.connect(
                driver="mysql",
                db_kwargs={
                    "uri": config.URI_MYSQL,
                },
            ) as con,
            con.cursor() as cursor,pa.memory_map(arrow_file, "rb") as source,
            pa.ipc.open_file(source) as reader,
        ):
            for i in range(reader.num_record_batches):
                batch = reader.get_batch(i)
                cursor.adbc_ingest(table_name, batch, mode="append")
    except Exception as e:
        print(f"DATABASE OPERATION FAILED: {e}")
        raise


# ============================================================
# csv_to_postgresql:
# params:
# ============================================================
def csv_to_postgresql(path: str, table_name: str, exists: bool) -> None:
    dataset = ds.dataset(path, format="csv")
    reader = dataset.scanner().to_reader()
    try:
        with (
            dbapi.connect(
                driver="postgresql",
                db_kwargs={"uri": config.URI_POSTGRESQL},
            ) as conn,
            conn.cursor() as cursor,
        ):
            first = not exists
            for batch in reader:
                cursor.adbc_ingest(
                    table_name, batch, mode="create" if first else "append"
                )
                first = False
            conn.commit()
    except Exception as e:
        print(f"{e}")
        raise


# ============================================================
# get_my_table:
# params:
# ============================================================
def get_my_table(arrow_file: str) -> config.MyArrowTable:
    with pa.memory_map(arrow_file, "rb") as source:
        return config.MyArrowTable(
            table = pa.ipc.open_file(source).read_all(),
            alias = Path(arrow_file).stem,
        )


# ============================================================
def main():
    mysql_create = get_query("SQL/OLTP/01_mysql_create.sql")
    with (
        dbapi.connect(
            driver="mysql",
            db_kwargs={"uri": config.URI_MYSQL,},
        ) as con,
        con.cursor() as cursor,
    ):
        cursor.execute("""
            DROP TABLE IF EXISTS USERS_01
        """)
        cursor.execute(mysql_create)
            
    with (
        dbapi.connect(
            driver="postgresql",
            db_kwargs={"uri": config.URI_POSTGRESQL},
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("""
            DROP TABLE IF EXISTS "USERS_01"
        """)
        conn.commit()
    
    sql = get_query("SQL/OLTP/01_oracledb.sql")
    oracledb_to_arrow(sql, "USERS_01")
    print("ARROW FILE LOADED: data/arrow/USERS.arrow")
    file = "data/arrow/USERS_01.arrow"
    arrow_to_mysql(file, "USERS_01")
    print("ARROW FILE data/arrow/USERS_01.arrow LOADED IN MYSQL DB")
    csv_to_postgresql("data/csv", "USERS_01", False)
    print("CSV FILES in data/csv LOADED INTO POSTGRESQL TABLE USERS_01")
    my_table = get_my_table(file)
    print(f'\nA TABLE NAMED -> {my_table.alias}\nWITH SCHEMA -> \n{my_table.table.schema}')


if __name__ == "__main__":
    main()
