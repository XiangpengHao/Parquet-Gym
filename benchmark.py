import toml
import subprocess
import os
import json
import time
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import psutil
from workloads.tpch_gen import tpch_gen


class IoCounters:
    def __init__(self, raw_counter) -> None:
        self.read_count = raw_counter.read_count
        self.write_count = raw_counter.write_count
        self.read_bytes = raw_counter.read_bytes
        self.write_bytes = raw_counter.write_bytes
        self.read_chars = raw_counter.read_chars
        self.write_chars = raw_counter.write_chars

    def __sub__(self, other):
        if not isinstance(other, IoCounters):
            raise TypeError("Unsupported operand type for -: " + type(other).__name__)

        result = IoCounters(self)
        result.read_count -= other.read_count
        result.write_count -= other.write_count
        result.read_bytes -= other.read_bytes
        result.write_bytes -= other.write_bytes
        result.read_chars -= other.read_chars
        result.write_chars -= other.write_chars

        return result

    def __str__(self):
        return f"IoCounters(read_count={self.read_count}, write_count={self.write_count}, read_bytes={self.read_bytes}, write_bytes={self.write_bytes}, read_chars={self.read_chars}, write_chars={self.write_chars})"

    def to_dict(self):
        return {
            "read_count": self.read_count,
            "write_count": self.write_count,
            "read_bytes": self.read_bytes,
            "write_bytes": self.write_bytes,
            "read_chars": self.read_chars,
            "write_chars": self.write_chars,
        }


class Metrics:
    def __init__(self, io: IoCounters, elapsed: float) -> None:
        self.io = io
        self.elapsed = elapsed

    def to_dict(self):
        return {
            "io": self.io.to_dict(),
            "elapsed": self.elapsed,
        }


def run_command_with_metrics(command, cwd) -> Metrics | None:
    try:
        start_time = time.time()

        proc = subprocess.Popen(command, cwd=cwd, shell=True)
        ps_proc = psutil.Process()
        old_counter = IoCounters(ps_proc.io_counters())

        proc.wait()

        elapsed_time = time.time() - start_time

        io_counters = IoCounters(ps_proc.io_counters())

        io_counters = io_counters - old_counter
        metric = Metrics(io_counters, elapsed_time)
        return metric
    except psutil.NoSuchProcess as e:
        print(f"Process finished before I/O stats could be retrieved: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return None


def run_command(command, cwd=None) -> float | None:
    try:
        start_time = time.time()
        subprocess.run(command, cwd=cwd, shell=True, check=True)
        elapsed_time = time.time() - start_time
        return elapsed_time
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while executing: {command}, err: {e}")
        return None


def clear_page_cache():
    run_command("echo 1 > /proc/sys/vm/drop_caches")


def build_reader(cwd, reader):
    build_steps = reader["build"]
    for step in build_steps:
        # Execute each command in the shell
        ok = run_command(step, cwd)
        if not ok:
            break


def benchmark_one(repeat, reader, workload) -> dict | None:
    workspace_dir = os.path.dirname(os.path.realpath(__file__))
    working_dir = os.path.join(workspace_dir, reader["working_dir"])

    executable_path = os.path.join(working_dir, reader["bin"])
    workload_path = os.path.join(workspace_dir, workload["path"])

    metrics = []
    for r in range(repeat):
        t = run_command_with_metrics(f"{executable_path} {workload_path}", working_dir)
        if not t:
            return None
        print(
            f"Iteration: {r}, elapsed time: {t.elapsed}s, reader: {reader['name']}, file: {workload['path']}"
        )
        metrics.append(t.to_dict())

    return {
        "reader": reader,
        "workload": workload,
        "result": metrics,
    }


def benchmark(global_config, readers, workloads):
    workspace_dir = os.path.dirname(os.path.realpath(__file__))
    results = []

    repeat = global_config["repeat"]

    for r in readers:
        working_dir = os.path.join(workspace_dir, r["working_dir"])
        build_reader(working_dir, r)
        for f in workloads:
            if global_config["clear_page_cache"]:
                clear_page_cache()
            rv = benchmark_one(repeat, r, f)
            results.append(rv)
    return results


def save_results(results, dst_dir):
    now = datetime.datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H-%M")

    date_directory = os.path.join(dst_dir, date_str)

    if not os.path.exists(date_directory):
        os.makedirs(date_directory)

    json_file = os.path.join(date_directory, f"{time_str}-results.json")
    try:
        with open(json_file, "w") as f:
            json.dump(results, f, indent=4)
            print(f"Json results saved to {json_file}")
    except Exception as e:
        print(f"Failed to save the json file: {e}")

    try:
        parquet_file = os.path.join(date_directory, f"{time_str}-results.parquet")
        table = pa.Table.from_pylist(results)
        pq.write_table(table, parquet_file)
        print(f"Parquet results saved to {parquet_file}")
    except Exception as e:
        print(f"Failed to save the parquet file: {e}")


def gen_tpch_if_not_exist():
    workspace_dir = os.path.dirname(os.path.realpath(__file__))
    data_dir = os.path.join(workspace_dir, "workloads", "tpch")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        tpch_gen(data_dir, sf=1)


if __name__ == "__main__":
    gen_tpch_if_not_exist()

    with open("config.toml") as f:
        config = toml.load(f)

    global_config = config["general"]
    workspace_dir = os.path.dirname(os.path.realpath(__file__))

    output_dir = os.path.join(workspace_dir, "results")
    results = benchmark(
        global_config=global_config,
        readers=config["readers"],
        workloads=config["workloads"],
    )
    save_results(results, output_dir)
