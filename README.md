# OneNab Data Pipeline Project

This repository contains Apache Airflow DAGs and Python scripts to process and load data for the OneNab data warehouse. It includes daily, aggregation, and master data pipelines built with Airflow and PySpark/pandas.

## Project Structure

```
.
├── OneNab
│   ├── dags
│   │   ├── Dag_OneNab_JobAllAOPDaily.py
│   │   ├── Dag_OneNab_JobDailyAGG.py
│   │   ├── Dag_OneNab_JobMaster.py
│   │   ├── Dag_OneNab_SSC_FactAllDataInvoice.py
│   │   ├── Dag_OneNab_SSC_FactAllDataInvoice_Delete.py
│   │   └── Dag_RTMSTG_SIN_All_Master.py
│   ├── files
│   │   ├── OneNab_SCC_AggActOutletYM.py
│   │   ├── OneNab_SCC_AggAssetCoolerYM.py
│   │   ├── OneNab_SSC_AggDataAssetYM.py
│   │   └── ...
│   ├── json
│   │   └── (configuration JSON files)
│   └── text
│       └── (text templates or logs)
├── README.md
```

## Prerequisites

* Python 3.7+
* Apache Airflow 2.x
* Database connectors (e.g., `pymssql`, `pyodbc`)
* pandas, PySpark, SQLAlchemy

## Installation

1. Clone the repository:

   ```bash
   ```

git clone <repo-url>
cd one-nab

````

2. (Optional) Create and activate a virtual environment:

   ```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
venv\\Scripts\\activate   # Windows
````

3. Install dependencies:

   ```bash
   ```

pip install -r requirements.txt

````

> If `requirements.txt` is not provided, install dependencies manually:

```bash
pip install apache-airflow pandas pyspark sqlalchemy pymssql
````

## Configuration

* Place custom JSON configuration files in `OneNab/json/`.
* Update Airflow connections and variables to match your environment.
* Ensure target databases and schemas exist and Airflow has proper credentials.

## Usage

1. Copy the `OneNab` directory to your Airflow `dags_folder`.
2. Start the Airflow scheduler and webserver:

   ```bash
   ```

airflow scheduler
airflow webserver

````

3. Trigger DAGs via the Airflow UI or CLI:

   ```bash
airflow dags trigger Dag_OneNab_JobAllAOPDaily
````

## DAGs Overview

| DAG Name                                   | Description                            |
| ------------------------------------------ | -------------------------------------- |
| `Dag_OneNab_JobAllAOPDaily`                | Daily AOP data processing and loading. |
| `Dag_OneNab_JobDailyAGG`                   | Daily aggregation pipelines.           |
| `Dag_OneNab_JobMaster`                     | Loads master dimension tables.         |
| `Dag_OneNab_SSC_FactAllDataInvoice`        | Processes and loads invoice fact data. |
| `Dag_OneNab_SSC_FactAllDataInvoice_Delete` | Cleans up old invoice data.            |
| `Dag_RTMSTG_SIN_All_Master`                | Staging master tables for RTM data.    |

## Data Processing Scripts

The `files` directory contains Python scripts that perform transformations and load data into target tables. Key scripts include:

* `OneNab_SCC_AggActOutletYM.py`: Aggregates active outlets by year-month.
* `OneNab_SSC_FactAllDataInvoice.py`: Processes and loads invoice fact data.
* `RTMSTG_Sales_ONENAB_SSC_DimCustomer.py`: Builds the customer dimension for sales data.

## Contributing

1. Fork the repository.
2. Create a feature branch:

   ```bash
   ```

git checkout -b feature/my-feature

````

3. Commit your changes:

   ```bash
git commit -m "Add new feature"
````

4. Push to your branch:

   ```bash
   ```

git push origin feature/my-feature

```

5. Open a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

```
