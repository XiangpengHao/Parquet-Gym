# Parquet Gym

🏋️ Exercise your parquet readers!


## Run benchmark
```bash
# Build docker image
docker build -t parquet-gym .

# Run benchmark
docker run -v ./results:/app/results parquet-gym 
```

The benchmark results are saved to `./results` folder, with one parquet file paired with a json file (human readable). 

## Gym equipments
### Supported Parquet readers
- [pyarrow](https://arrow.apache.org/docs/python/parquet.html)
- [arrow-rs](https://docs.rs/parquet/latest/parquet/)

todo: 

### Supported workloads
- Sample data.

todo: TPC-H, NYX-taxi, etc.

### Supported metrics
- Elapsed time (wall time)

todo: CPU time, memory usage, disk IO bandwidth, disk IO count.

