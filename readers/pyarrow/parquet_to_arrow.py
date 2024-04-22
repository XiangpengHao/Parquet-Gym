import argparse
import pyarrow.parquet as pq

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Converts Parquet files to Arrow format")
    parser.add_argument('input', type=str, help="The input Parquet file path")
    parser.add_argument('-p', '--print', action='store_true', help="Print the record batch")
    
    # Parse arguments
    args = parser.parse_args()

    # Read Parquet file
    try:
        table = pq.read_table(args.input)
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return

    # Optionally print the record batch
    if args.print:
        print(table)

if __name__ == "__main__":
    main()
