# Copied from https://github.com/ljishen/tpch-data
# Very cool/elegant way to generate TPC-H data

import argparse
import duckdb
import pyarrow.parquet as pq
import os


def tpch_gen(dir, sf=1):
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL tpch; LOAD tpch")
    con.execute(f"CALL dbgen(sf={sf})")
    print(con.execute("show tables").fetchall())
    tables = [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]
    for t in tables:
        print("Writing table", t)
        res = con.query("SELECT * FROM " + t)
        path = os.path.join(dir, t + ".parquet")
        pq.write_table(res.to_arrow_table(), path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate TPCH data and write to Parquet files."
    )
    parser.add_argument(
        "--sf", type=int, default=1, help="Scaling factor for the TPCH data generation"
    )
    args = parser.parse_args()
    current_dir = os.path.dirname(os.path.realpath(__file__))
    tpch_gen(current_dir, sf=args.sf)
