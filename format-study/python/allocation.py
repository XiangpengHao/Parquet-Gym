import json
import pandas as pd
import seaborn as sns
import argparse
import matplotlib.pyplot as plt
import plot_result


def plot(with_mimalloc: str, without_mimalloc: str, output: str):
	with_mimalloc = plot_result.load_data(with_mimalloc)
	without_mimalloc = plot_result.load_data(without_mimalloc)
	data = pd.concat([with_mimalloc, without_mimalloc])

	data = data[data['num_columns'] == 100_000]
	fig, ax1 = plt.subplots(1, 1, figsize=(6, 4))
	sns.barplot(data=data, x='mimalloc', y='time_per_column', ax=ax1, color='tab:blue')
	ax1.set_xlabel('')
	ax1.set_ylabel('Time per column (ns)')
	ax1.set_xticklabels(['Default allocator', 'Mimalloc'])
	fig.show()
	fig.savefig(output, dpi = 300)



if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Plotting script')
	parser.add_argument('with_mimalloc', type=str, help='Path to the input file with mimalloc')
	parser.add_argument('without_mimalloc', type=str, help='Path to the input file without mimalloc')
	args = parser.parse_args()

	plot(args.with_mimalloc, args.without_mimalloc, 'python/allocation.png')
