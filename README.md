# Distributed Healthcare Analytics with PySpark

This project uses PySpark to process and analyze a synthetic healthcare dataset in a distributed setting.  
The goal is to extract meaningful insights about hospitals, patients, and doctors, and to compare execution performance across local and cluster modes.

---

## Dataset

The dataset contains 55 500 rows and 15 columns (≈ 8 MB).  
Each row represents a patient admission with fields such as:

- Name, Age, Gender, Blood Type  
- Medical Condition  
- Date of Admission, Discharge Date  
- Doctor, Hospital, Room Number  
- Insurance Provider  
- Billing Amount  
- Admission Type (Emergency, Urgent, Elective)  
- Medication, Test Results  

An additional `ID` column is generated with `monotonically_increasing_id()`.

---

## Main Analyses

All analyses are implemented with PySpark DataFrames and standard SQL-like operations.

### 1. Hospital Performance Evaluation

- Average billing amount per hospital  
- Average stay duration using `Stay Duration = Discharge Date - Date of Admission`  
- Distribution of admission types (Emergency, Urgent, Elective) per hospital  
- Computation of a custom KPI Index based on normalized metrics:
  - Average Billing  
  - Average Stay Duration  
  - Counts of Emergency, Urgent, and Elective admissions  
- Ranking of hospitals by KPI and identification of:
  - Top 10 hospitals by KPI  
  - Top 5 hospitals by number of emergency cases  

### 2. Seasonal Trends

- Creation of a `Season` column from month of admission (Winter, Spring, Summer, Autumn)  
- Most common medical condition per season  
- Total admissions per season and identification of the busiest season  

### 3. Demographic Analyses

- Distribution of patients by medical condition (pie chart)  
- For each age, top medical condition and corresponding number of patients  
- Comparison of medical conditions by gender (Male/Female) with grouped bar charts  

### 4. Length of Stay and Admission Trends

- For each (Medical Condition, Admission Type) pair:
  - Patient count  
  - Total length of stay  
  - Average length of stay  
- Top 5 condition–admission-type combinations by total length of stay  
- Average daily admissions per admission type  

### 5. Emergency Admission Trends

- Emergency-only subset  
- Admissions grouped by day of week  
- Identification of the top 2 busiest days for emergency admissions  
- Visualization of the weekly admission distribution (pie chart)

---

## Clustering

### 6. Patient Clustering

Features used:

- Age  
- Stay Duration  
- Billing Amount  
- Medical Condition (tokenized and hashed)  

Pipeline:

- Tokenizer → HashingTF → VectorAssembler → StandardScaler  
- KMeans clustering for k = 2 to 10, evaluated with Silhouette score  
- Best k selected automatically (here, k = 6)  
- Aggregated statistics per cluster:
  - Number of patients  
  - Average, minimum, and maximum age  
  - Average, minimum, and maximum stay duration  
  - Average, minimum, and maximum billing amount  

### 7. Doctor Clustering

For each doctor:

- Number of patients  
- Average billing amount  
- Average stay duration  
- Number of medical conditions treated  
- Aggregated list of medical conditions (flattened and tokenized)  

Features are assembled and scaled similarly, then clustered with KMeans:

- k from 2 to 10, Silhouette-based selection (best k = 2)  
- For each doctor cluster:
  - Number of doctors  
  - Average number of patients  
  - Average billing amount  
  - Average stay duration  
  - Average number of unique medical conditions treated  

---

## Performance Evaluation

Execution environments compared:

- Cluster mode (university cluster, 10 nodes)  
- Local mode on a single machine:
  - `local[1]` (1 core)  
  - `local[2]` (2 cores)  
  - `local[*]` (all available cores)  

Average execution times over 10 runs:

| Mode       | Avg Execution Time (ms) |
|-----------|-------------------------|
| cluster   | ≈ 413 017              |
| local[1]  | ≈ 39 008               |
| local[2]  | ≈ 31 258               |
| local[*]  | ≈ 28 624               |

Conclusions:

- For this small dataset (≈ 8 MB), cluster mode is slower due to communication and coordination overhead.  
- Local execution, especially `local[*]`, is significantly faster.  
- Distributed cluster execution becomes advantageous only for larger datasets where parallelism outweighs overhead.  

---

## Requirements

- Python 3  
- PySpark  
- NumPy  
- Matplotlib  

Example installation:

```bash
pip install pyspark numpy matplotlib
