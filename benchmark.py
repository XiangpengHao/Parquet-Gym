import toml
import subprocess
import os


def run_command(command, cwd) -> bool:
    try:
        # Run the command and wait for it to complete
        subprocess.run(command, cwd=cwd, shell=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while executing: {command}")
        print(f"Error: {e}")
        return False


def build_reader(cwd, reader):
    build_steps = reader["build"]
    for step in build_steps:
        # Execute each command in the shell
        ok = run_command(step, cwd)
        if not ok:
            break


def benchmark(readers, files):
    workspace_dir = os.path.dirname(os.path.realpath(__file__))
    for r in readers:
        working_dir = os.path.join(workspace_dir, r["working_dir"])
        build_reader(working_dir, r)
        for f in files:
            executable = os.path.join(working_dir, r["bin"])
            workload_file = os.path.join(workspace_dir, f)

            ok = run_command(f"{executable} {workload_file}", working_dir)
            if not ok:
                break


if __name__ == "__main__":
    with open("config.toml") as f:
        config = toml.load(f)

    files = [x["path"] for x in config["workloads"]]
    benchmark(readers=config["readers"], files=files)
