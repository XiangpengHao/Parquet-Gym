import argparse
import subprocess
import os
import json
import shutil
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from time import sleep

columns = [10, 100, 500, 1000, 3000]
stats_options = ["chunk", "none", "page"]

def generate(output_dir):
    for column in columns:
        for stats in stats_options:
            output_file = f"{output_dir}/{column}col_1m_{stats}.parquet"
            command = [
                "cargo", "run", "--bin", "generator", "--release", "--",
                "--column", str(column),
                "--row-per-group", "1000000",
                "--stats", stats,
                "--output", output_file
            ]
            print(f"Running command: {' '.join(command)}")
            subprocess.run(command)

def benchmark(parquet_dir, output_dir):
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    for column in columns:
        for stats in stats_options:
            input_file = f"{parquet_dir}/{column}col_1m_{stats}.parquet"
            command = [
                "cargo", "run", "--bin", "wide_table_bench", "--release", "--",
                "--input", input_file, "--output-dir", output_dir,
            ]
            print(f"Running command: {' '.join(command)}")
            subprocess.run(command) 
            sleep(1)

def load_data(input_dir):

    def stats_type_from_file_name(file_name):
        return file_name.split("_")[-1].split(".")[0]
    records = []
    for f in os.listdir(input_dir):
        if f.endswith(".json"):
            with open(os.path.join(input_dir, f)) as json_f:
                records.extend(json.load(json_f)[1:])
    df = pd.DataFrame(records)
    df['time_per_column'] = df['metadata_decode_time_nanos'] / df['column_cnt']
    df['stats'] = df.apply(lambda x: stats_type_from_file_name(x['file_name']), axis=1)
    return df

def plot(input_dir, output_dir):
    df = load_data(input_dir) 
    fig, ax1 = plt.subplots(1, 1, figsize=(6, 4))
    sns.lineplot(data=df, x='column_cnt', y='metadata_decode_time_nanos', hue='stats', ax=ax1, markers=True, style='stats', dashes=False)
    ax1.legend(title='Stats level')
    ax1.set_xlabel('Number of columns')
    ax1.set_ylabel('Metadata decode time (ns)')
    fig.savefig(os.path.join(output_dir, "metadata_decode_time.png"), dpi=300)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and benchmark Parquet files")
    parser.add_argument("action", choices=["generate", "benchmark", "plot"], help="Action to perform")
    parser.add_argument("--output_dir", help="Directory for the output files")
    parser.add_argument("--input_dir", help="Directory for the input files", default="")

    args = parser.parse_args()

    if args.action == "generate":
        generate(args.output_dir)
    elif args.action == "benchmark":
        benchmark(args.input_dir, args.output_dir)
    elif args.action == "plot":
        plot(args.input_dir, args.output_dir)
