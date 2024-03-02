<meta name="robots" content="noindex,nofollow">

# Spanning Tree-based Query Plan Enumeration

## Table of Contents
1. [Experimental Setup](#setup)
2. [Install PostgreSQL](#installation)
3. [IMDB Dataset](#dataset)
4. [Join Order Benchmark (JOB)](#benchmark)
5. [Graph Topologies](#topology)
6. [Evaluation](#evaluation)
7. [Questions](#questions)
8. [Acknowledgments](#acknowledgments)
9. [References](#references)
10. [Citation](#citation)

## 1. Experimental Setup <a name="setup"></a>
Current work was developed in the following environment:
- Machine: 2x Intel(R) Xeon(R) CPU E5-2660 v4 (56 CPU cores and 256GB memory)
- OS: Ubuntu 22.04 LTS
- Python 3.10.6
- Docker v20.10.21, NVIDIA Docker v2.13.0

## 2. Install PostgreSQL <a name="installation"></a>
We collected cardinality estimations from:
- [PostgreSQL](https://www.postgresql.org) v15.1

To replicate the PostgreSQL docker image for runtime time comparison, one can follow these [instructions](PostgreSQL_README.md).

## 3. IMDB Dataset <a name="dataset"></a>
The dataset that was used is [Internet Movie Data Base (IMDB)](https://www.imdb.com/). The original data is publicly available (ftp://ftp.fu-berlin.de/pub/misc/movies/database/) in txt files, and the open-source [imdbpy package](https://bitbucket.org/alberanid/imdbpy/get/5.0.zip) was used to transform txt files to CSV files in [[1]](#1). See more details [here](https://github.com/gregrahn/join-order-benchmark). This 3.6GB snapshot is from May 2013, and it can be downloaded from [here](homepages.cwi.nl/~boncz/job/imdb.tgz). The dataset includes 21 CSV files i.e., 21 relations in total. The package also includes queries to create the necessary relations written in `schema.sql` or `schematext.sql` files. Lastly, in addition to primary keys, there are queries to create foreign keys in case one decides to use them.

## 4. Join Order Benchmark (JOB) <a name="benchmark"></a>
The workload used to evaluate current work is [Join Order Benchmark (JOB)](http://www-db.in.tum.de/~leis/qo/job.tgz).
- JOB consists of 113 queries in total, including 33 query families with equijoins, and each family's queries differ only in selection predicates. Join sizes 2-17, join predicates 4-28, and tables 2-17. JOB query topologies include linear, cyclic, star and clique. We provide the benchmark queries [here](input_data/job/workload_queries).

## 5. Graph Topologies <a name="topology"></a>
Additionally, we generated queries with four different join graph topologies using [mutable](#2). The source code is available [here](https://github.com/mutable-org/mutable). We provide the queries [here](input_data/topology/workload_queries).

NOTE: Due to the large file limitation in GitHub, file `results_estimates-topology.csv` is not available. However, one may re-generate the cardinalities using [mutable](#2).

## 6. Evaluation <a name="evaluation"></a>
We provide [cost-Python-scripts](scripts_cost) to run each enumeration algorithm using C_mm and C_out cost functions. Their optimization time, costs, selected edge sequences, and join operators are stored [here](output_data/job/costs/). In addition, the query plans for IK-KBZ, LinearizedDP, GOO, A*, and optimal plans (obtained by DPcpp) are executed and collected from [mutable](#2). The [input data](input_data/) and step-by-step instructions on how to run these [cost-Python-scripts](scripts_cost) are provided [here](MST_enum_README.md).

We provide [runtime-Python-scripts](scripts_runtime) to execute query plans obtained by each enumeration algorithm. Their PostgreSQL optimization time, execution time, and total runtime are stored [here](output_data/job/runtime/). The execution times are represented in milliseconds. The [input data](input_data/job) and [query plans](output_data/job/costs/), as well as step-by-step instructions on how to run these [runtime-Python-scripts](scripts_runtime) are provided [here](MST_runtime_README.md).

To reproduce the figures from the paper we provide [figure-Python-scripts](figures). The [cost input data](output_data/job/costs) for C_mm, [cost input data](output_data/topology/costs) for C_out and [runtime input data](output_data/job/runtime) as well as step-by-step instructions on how to run these [figure-Python-scripts](figures) are provided [here](MST_figure_README.md).

## 7. Questions <a name="questions"></a>
If you have questions, please contact:
- Yesdaulet Izenov [yesdaulet.izenov@nu.edu.kz], (https://yizenov.github.io/)
- Asoke Datta [adatta2@ucmerced.edu], (https://asoke26.github.io/adatta2/)
- Brian Tsan [btsan@ucmerced.edu], (https://github.com/btsan)
- Abylay Amanbayev [amanbayev@ucmerced.edu], (https://kz.linkedin.com/in/aabylay)
- Florin Rusu [frusu@ucmerced.edu], (https://faculty.ucmerced.edu/frusu/)

## 8. Acknowledgments <a name="acknowledgments"></a>
This work is supported by [NSF award (number 2008815)](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2008815&HistoricalAwards=false).

## 9. References <a name="references"></a>
<a id="1">[1]</a> [Query optimization through the looking glass, and what we found running the Join Order Benchmark](https://doi.org/10.1007/s00778-017-0480-7)</br>
<a id="2">[2]</a> [mutable: A Database System for Research and Fast Prototyping](https://mutable.uni-saarland.de)</br>

## 10. Citation <a name="citation"></a>
```bibtex
@misc{este-github,
  author = {Yesdaulet Izenov},
  title = "{Spanning Tree-based Query Plan Enumeration}",
  howpublished = "\url{https://github.com/yizenov/este}"
}
```
