import json
import pandas as pd
import seaborn as sns
import argparse
import matplotlib.pyplot as plt


def load_data(input: str):
    with open(input) as f:
        data = json.load(f)

    records = []
    for result in data:
        config = result['config']
        measurements = result['measurements']
        elapsed_time = measurements['elapse']['secs'] * 1e9 + measurements['elapse']['nanos']
        records.append({
            'num_columns': config['num_columns'],
            'elapse': elapsed_time,
            'meta_data_size': measurements['meta_data_size'],
            'mimalloc': config['mimalloc'],
        })

    df = pd.DataFrame(records)
    df['time_per_column'] = df['elapse'] / df['num_columns']
    return df



def plot(input: str, output: str):
    df = load_data(input)
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(12, 4))

    sns.barplot(data=df, x='num_columns', y='elapse', ax=ax1, color='tab:blue')
    ax1.set_xlabel('Number of columns')
    ax1.set_ylabel('Elapsed time (nanoseconds)', color='tab:blue')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    ax1.set_yscale('log')

    ax2.set_xlabel('Number of columns')
    ax2.set_ylabel('Metadata size (bytes)', color='tab:green')
    sns.barplot(data=df, x='num_columns', y='meta_data_size', ax=ax2, color='tab:green')
    ax2.tick_params(axis='y', labelcolor='tab:green')
    ax2.set_yscale('log')

    ax3.set_xlabel('Number of columns')
    ax3.set_ylabel('Time per column (nanoseconds)', color='tab:orange')
    sns.barplot(data=df, x='num_columns', y='time_per_column', ax=ax3, color='tab:orange')
    ax3.tick_params(axis='y', labelcolor='tab:orange')

    fig.tight_layout()
    fig.savefig(output, dpi = 300)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plotting script')
    parser.add_argument('input', type=str, help='Path to the input file')
    parser.add_argument('--output', type=str, default='python/metadata.png', help='Path to the output file')

    args = parser.parse_args()

    plot(args.input, args.output)

