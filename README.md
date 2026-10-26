# Advanced Database Systems – Big Data Analytics with Apache Spark

Semester project for the **Advanced Database Systems** course (School of Electrical and Computer Engineering, NTUA, 2025).

We analyze the **Los Angeles Crime Data** (2010–2025) together with several auxiliary datasets using **Apache Spark** and **Apache Sedona**, on a Spark cluster in AWS. Each query is implemented with more than one Spark API or execution strategy. We compare execution times and physical plans across:
- APIs: DataFrame, SQL and RDD
- join strategies: broadcast, sort-merge, shuffle hash and nested loop
- cluster configurations: number of executors, cores and memory

## Repository contents

| File | Description |
| --- | --- |
| [`Advanced_DataBase_Systems_Queries.ipynb`](Advanced_DataBase_Systems_Queries.ipynb) | All queries, with outputs, execution plans and timings |
| [`Advanced_Database_Systems_Report.pdf`](Advanced_Database_Systems_Report.pdf) | Project report (in Greek) with the analysis of every query |
| [`advanced_db_project_2025.pdf`](advanced_db_project_2025.pdf) | The project assignment |

## Tech stack

- **Apache Spark (PySpark)**: DataFrame, Spark SQL and RDD APIs
- **Apache Sedona**: geospatial types, functions and spatial joins
- **AWS**: notebooks run via the Sparkmagic (PySpark) kernel against a remote Spark cluster through Livy; data read from S3

## Datasets

- `LA_Crime_Data_2010_2019.csv`, `LA_Crime_Data_2020_2025.csv`: crime incidents in Los Angeles
- `LA_Police_Stations.csv`: police station locations
- `LA_income_2021.csv`: median household income per ZIP code
- `LA_Census_Blocks_2020.geojson`: 2020 census blocks (population, housing units, community)
- `MO_codes.txt`: Modus Operandi code descriptions
- `RE_codes.csv`: victim descent codes

## Queries and results

### Query 1: Victim age groups in aggravated assaults
We count victims of aggravated assault per age group (children, young adults, adults, elderly), sorted by count.

| Implementation | Time (s) |
| --- | --- |
| DataFrame without UDF (native expressions) | **10.56** |
| DataFrame with Python UDF | 11.92 |
| RDD API | 16.45 |

Native expressions stay inside the Catalyst optimizer. A Python UDF adds per-row serialization overhead, and the RDD API gets no optimization at all.

### Query 2: Top-3 victim descent groups per year
For every year we keep the 3 most frequent victim descent groups and their percentage of that year's victims, using window functions.

| Implementation | Time (s) |
| --- | --- |
| DataFrame API | **20.89** |
| SQL API (temp views) | 30.21 |

### Query 3: MO code frequency and join strategies
We explode the `Mocodes` column, join it with the MO code dictionary and rank codes by frequency. The same join is forced to each strategy with hints.

| Implementation / hint | Time (s) |
| --- | --- |
| DataFrame (default → broadcast hash join) | 4.42 |
| DataFrame (broadcast hint) | 4.20 |
| DataFrame (sort-merge join) | 4.44 |
| DataFrame (shuffle hash join) | 7.59 |
| DataFrame (shuffle replicate NL hint → falls back to broadcast hash join) | 3.89 |
| RDD API | 17.53 |

When a big table is joined with a small one, the broadcast hash join avoids the shuffle entirely, so it is the best choice.

### Query 4: Nearest police station per crime (Sedona)
We represent crimes and stations as geometric points and find each crime's closest station by spherical distance. Then, per police division, we compute the average distance and the number of crimes.

| Cores / memory per executor | Time (s) |
| --- | --- |
| 1 core, 2 GB | 2.23 |
| 2 cores, 4 GB | 2.12 |
| 4 cores, 8 GB | **1.97** |

### Query 5: Income vs crime per capita across LA communities
We compute per-capita income per community from the census blocks and the ZIP-level income data. A spatial join (`ST_Contains`) assigns the 2020–2021 crimes to census blocks. We then study the correlation between income and crimes per capita for:
- all communities
- the top-10 richest communities
- the bottom-10 poorest communities

Each analysis runs under three cluster configurations with 8 cores and 16 GB in total. Times are in seconds.

| Configuration | All communities | Top-10 | Bottom-10 |
| --- | --- | --- | --- |
| 2 executors × 4 cores, 8 GB | 86.16 | 83.46 | 92.45 |
| 4 executors × 2 cores, 4 GB | 82.58 | 92.23 | 83.29 |
| 8 executors × 1 core, 2 GB | **70.97** | 94.98 | 81.13 |

Over all communities, income and crime per capita are almost uncorrelated (r ≈ −0.067).

## Running the notebook

The notebook targets a remote Spark cluster through the **Sparkmagic (PySpark)** kernel, with Apache Sedona available on the cluster.
1. Upload the datasets to your own S3 bucket under `project_data/`.
2. Replace `your-s3-bucket` in the notebook's paths with the name of your bucket.

Every query cell builds its own `SparkSession` with the executor configuration used for that experiment.

## Authors

- Giorgos Tzamouranis ([@giorgostzamouranis](https://github.com/giorgostzamouranis))
- Nikolaos Katsaidonis ([@NikosK10](https://github.com/NikosK10))
