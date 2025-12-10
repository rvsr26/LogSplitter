# 🚀 MapReduce Log Analysis Engine (Parallel Processing)

This project demonstrates a high-performance log analysis pipeline implemented in Python using the **MapReduce** pattern and the `multiprocessing` library. It is designed to efficiently process large volumes of mock log data by leveraging all available CPU cores.

## 🌟 Features

* **Parallel Processing:** Uses `multiprocessing.Pool` to divide the log analysis workload across all available CPU cores.
* **MapReduce Implementation:** Clearly separates the data processing into `Mapper` (counting) and `Reducer` (aggregating) stages for scalable architecture.
* **In-Worker Data Generation:** Data generation is handled inside the mapper functions, minimizing the memory footprint and inter-process communication overhead for extremely large log sizes.
* **Key Metrics Calculation:** Calculates top suspicious IP addresses, total requests, and server error rates (500s).

## 🛠️ Requirements

* Python 3.6+ (Standard libraries only)

## 📦 How to Run

1.  Save the provided Python code as `log_analyzer.py`.
2.  Run the script from your terminal:

    ```bash
    python log_analyzer.py
    ```

## ⚙️ Configuration

The core parameters for the analysis are set at the top of the `log_analyzer.py` file:

| Constant | Default Value | Description |
| :--- | :--- | :--- |
| `LOG_SIZE` | `5_000_000` | The total number of log lines to generate and analyze. |
| `NUM_WORKERS` | `multiprocessing.cpu_count()` | The number of CPU cores to use for parallel mapping. |
| `IPS` | `192.168.1.1` to `192.168.1.49` | List of standard IP addresses used for mock logs. |
| `STATUS_CODES` | `[200, 404, 500, 301]` (with weighting) | List of possible HTTP status codes. |

## 🧠 MapReduce Explained

This program utilizes the classic MapReduce paradigm to handle the large dataset. 

[Image of MapReduce architecture]


### 1. The Map Phase (`mapper_function`)

The `mapper_function` is run **in parallel** by each worker.

* It **generates** its assigned chunk of logs (local data generation).
* It **parses** each line using a regular expression.
* It produces **partial counts** for IPs and Status Codes within its local chunk.

### 2. The Reduce Phase (`reducer_function`)

The `reducer_function` is run **sequentially** by the main process using `functools.reduce`.

* It takes all the partial count dictionaries from the mappers.
* It **aggregates** (reduces) these results by summing the counts for identical keys (IPs or Status Codes) into a single final dictionary.

## 📊 Sample Output

Running the script will produce output similar to this (timing and counts will vary based on your system and CPU):