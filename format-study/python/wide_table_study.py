import argparse
import subprocess
import os
import json
import shutil
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from time import sleep
import concurrent.futures
from matplotlib.patches import Patch

columns = [10, 100, 500, 1_000, 2_000, 5_000, 10_000, 50_000, 100_000]
# columns = [100_000]
stats_options = ["chunk", "none", "page"]

def generate(output_dir):
    def run_command(command):
        print(f"Running command: {' '.join(command)}")
        subprocess.run(command)

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for column in columns:
            for stats in stats_options:
                output_file = f"{output_dir}/{column}col_10b_{stats}.parquet"
                command = [
                    "cargo", "run", "--bin", "generator", "--release", "--",
                    "--column", str(column),
                    "--value-cnt-million", "10000",
                    "--stats", stats,
                    "--output", output_file
                ]
                futures.append(executor.submit(run_command, command))
        for future in concurrent.futures.as_completed(futures):
            future.result()


def benchmark(parquet_dir, output_dir):
    for column in columns:
        for stats in stats_options:
            input_file = f"{parquet_dir}/{column}col_10b_{stats}.parquet"
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
    df['time_per_column'] = df['metadata_end_to_end_load_time_nanos'] / df['column_cnt']
    df['stats'] = df.apply(lambda x: stats_type_from_file_name(x['file_name']), axis=1)
    return df

hue_order = ['none', 'chunk', 'page']

def plot_line(df, ax1, ax2):
    sns.lineplot(data=df, x='column_cnt', y='metadata_end_to_end_load_time_nanos', hue='stats', 
                 ax=ax1, hue_order=hue_order, markers=True, style='stats', dashes=False)
    ax1.legend(title='Stats level', frameon=False)
    ax1.set_xlabel('Number of columns')
    ax1.set_ylabel('Metadata decode time (ms)')
    ax1.set_yscale('log')
    ax1.set_xscale('log')
    ax1.set_title('Metadata decode time (trend)')
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: '{:.0f}'.format(x/1e6)))

    sns.lineplot(data=df, x='column_cnt', y='metadata_len', hue='stats', 
                 ax=ax2, hue_order=hue_order, markers=True, style='stats', dashes=False)
    ax2.legend(title='Stats level', frameon=False)
    ax2.set_xlabel('Number of columns')
    ax2.set_ylabel('Metadata size (MB)')
    ax2.set_xscale('log')
    ax2.set_yscale('log')
    ax2.set_title('Metadata size (trend)')
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: '{:.0f}'.format(x/1e6)))


def plot_bar(df, ax1, ax2):
    column_subset = [1_000, 2_000, 5_000, 10_000]
    df_subset = df[df['column_cnt'].isin(column_subset)]

    palette = sns.color_palette()[:3]

    sns.barplot(data=df_subset, x='column_cnt', y='metadata_end_to_end_load_time_nanos', hue='stats',
                hue_order=hue_order, ax=ax1, palette=palette)
    sns.barplot(data=df_subset, x='column_cnt', y='metadata_len', hue='stats', 
                hue_order= hue_order,ax=ax2)
    ax1.set_xlabel('Number of columns')
    ax2.set_xlabel('Number of columns')
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: '{:.0f}'.format(x/1e6)))
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: '{:.0f}'.format(x/1e6)))

    darker_palette = [sns.set_hls_values(c, l=.3) for c in palette]
    sns.barplot(data=df_subset, x='column_cnt', y='schema_build_time_nanos', hue='stats',
                hue_order=hue_order, ax=ax1, palette=darker_palette)

    ax1.set_ylabel('Metadata decode time (ms)')
    ax2.set_ylabel('Metadata size (MB)')

    h, l = ax1.get_legend_handles_labels()
    ax1.legend(title='Stats level', labels=l[:3], handles=h[:3], frameon=False)

    
    ax1.annotate('Thrift decode', xy=(2, 20_000_000), xytext=(0.3, 0.4), 
                    textcoords='axes fraction', arrowprops=dict(facecolor='black', arrowstyle='->'))

    ax1.annotate('Schema build', xy=(2, 1_000_000), xytext=(0.2, 0.2), 
                    textcoords='axes fraction', arrowprops=dict(facecolor='black', arrowstyle='->'))

    ax2.legend(title='Stats level', frameon=False)
    ax1.set_title('Metadata decode time')
    ax2.set_title('Metadata size')


def plot_all(input_dir, output_dir):
    df = load_data(input_dir) 
    fig, (ax2, ax1) = plt.subplots(2, 2, figsize=(10, 8))
    fig.subplots_adjust(hspace=0.4)

    plot_line(df, ax1[0], ax1[1])
    plot_bar(df, ax2[0], ax2[1])

    fig.savefig(os.path.join(output_dir, "metadata_decode_time.png"), dpi=300)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    plot_line(df, ax1, ax2)
    fig.savefig(os.path.join(output_dir, "metadata_decode_trend.png"), dpi=300)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    plot_bar(df, ax1, ax2)
    fig.savefig(os.path.join(output_dir, "metadata_decode_bar.png"), dpi=300)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and benchmark Parquet files")
    parser.add_argument("action", choices=["generate", "benchmark", "plot", "plot_trend"], help="Action to perform")
    parser.add_argument("--output_dir", help="Directory for the output files")
    parser.add_argument("--input_dir", help="Directory for the input files", default="")

    args = parser.parse_args()

    output_dir = args.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if args.action == "generate":
        generate(output_dir)
    elif args.action == "benchmark":
        benchmark(args.input_dir, output_dir)
    elif args.action == "plot":
        plot_all(args.input_dir, output_dir)
