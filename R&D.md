A scratchpad mostly for my own thoughts.

### Design discussions
We need three folders: (1) readers, (2) workload, (3) predicates.

We should use a Docker file to setup the environment.
Then we will use bash or python script to run the benchmark. We should use bash whenever possible, but may potentially invoke python scripts.

### Design goals
- We should make it super simple/intuitive for user to run the benchmark. Consider cognitive burden! Think about how hackers news reader click into this repo, how should we convince them to not close the tab immediately? 
- We should make it easy for users to add their own readers/workload/predicates.
- Benchmark results should be in Parquet format!

#### Cognitive burden
- As few terminology as possible.
- README should contain self-explanatory figures. README should be short, concise, and to the point.
- Code should be very clean and easy to read. We want people to modify the benchmark code to fit their own needs.

