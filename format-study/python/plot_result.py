import json
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Read the JSON file
with open('target/benchmark/metadata_bench.json') as f:
    data = json.load(f)

# Flatten the JSON structure and load it into a DataFrame
records = []
for result in data:
    config = result['config']
    measurements = result['measurements']
    elapsed_time = measurements['elapse']['secs'] * 1e9 + measurements['elapse']['nanos']
    records.append({
        'num_columns': config['num_columns'],
        'elapse': elapsed_time,
        'meta_data_size': measurements['meta_data_size']
    })

df = pd.DataFrame(records)


fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4))

sns.barplot(data=df, x='num_columns', y='elapse', ax=ax1, color='tab:blue')
ax1.set_xlabel('Number of columns')
ax1.set_ylabel('Elapsed time (seconds)', color='tab:blue')
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.set_yscale('log')

ax2.set_xlabel('Number of columns')
ax2.set_ylabel('Metadata size (bytes)', color='tab:green')
sns.barplot(data=df, x='num_columns', y='meta_data_size', ax=ax2, color='tab:green')
ax2.tick_params(axis='y', labelcolor='tab:green')
ax2.set_yscale('log')

fig.tight_layout()
fig.savefig('./python/metadata.png', dpi = 300)
