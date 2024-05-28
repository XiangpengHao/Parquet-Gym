# Parquet Format Studies

Here we want to study the parquet format, specifically, answer the following questions:
1. How expensive it is to decode parquet file with wide columns (more than 10k columns)? 



### Wide-table study

#### Why?
Use case 1:
Parquet is used in machine learning workloads to store [vector embeddings](https://huggingface.co/datasets?sort=downloads&search=embed), each vector is an array of floating numbers.
For a vector with 10k dimensions, we have 10k columns in the parquet schema. (This is not true: vector embeddings are stored as lists in Parquets' nested model.)
(personal note: I don't think this is the intended use case for parquet)

Use case 2:  it is common to store a large number of features (thousands of key-value pairs) for ML training in ORC/Parquet format.
(Section 5.5 in paper: https://www.vldb.org/pvldb/vol17/p148-zeng.pdf)




