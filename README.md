# Google Books ETL Pipeline

### Overview

Developed a containerized ETL pipeline using Docker and Airflow, extracting ~30 book records daily from the Google Books API. Transformed raw data via Pandas, enforcing schema consistency and conducting critical data quality validations before bulk loading into PostgreSQL. Executed Exploratory Data Analysis (EDA) to identify trends and support downstream decision-making.

### Architecture

[Architecture Diagram](https://github.com/rtriders/Google_books_data_pipeline/blob/main/images/Pipeline_design.png)

### Services Used

1. [**Apache Airflow**](https://airflow.apache.org/docs/apache-airflow/stable/index.html): Apache Airflow is an open-source platform for **developing, scheduling, and monitoring batch-oriented workflows**. It enables users to programmatically author workflows using Python (via DAGs), and offers a web-based UI to visualize, track, and manage execution.:contentReference[oaicite:0]{index=0}  
2. [**Pandas**](https://pandas.pydata.org/docs/): Pandas is a fast, powerful, open-source Python library offering **high-performance, easy-to-use data structures (like Series and DataFrame) and analysis tools** that simplify data manipulation, cleaning, and transformation.:contentReference[oaicite:1]{index=1}  
3. [**Docker**](https://docs.docker.com/): Docker is a software platform enabling developers to **build, test, and deploy applications quickly using containers**, which are lightweight, isolated environments bundling code, dependencies, and runtime for consistency across environments.:contentReference[oaicite:2]{index=2}  
4. [**PostgreSQL**](https://www.postgresql.org/about/): PostgreSQL is a powerful, open-source, object-relational database system known for its **reliability, data integrity, extensibility, and enterprise-level performance**, making it ideal for analytical and transactional workloads.:contentReference[oaicite:3]{index=3}


### Monitoring via Airflow UI
- **Graph View**: Visual depiction of task dependencies and states (success, running, failed).  
- **Grid View**: Heatmap showing status of each task across multiple runs—ideal for spotting trends or failures at a glance.  
- **Task Instance View**: Clicking any task opens detailed views including logs, execution metadata, XComs, and rendered templates. This is your go-to for debugging.:contentReference[oaicite:4]{index=4}

### Logs Output
- Logs are generated per task instance and accessible directly in the UI.
- Airflow supports custom handler configurations, including local log storage and options for remote storage like S3 or GCS.:contentReference[oaicite:5]{index=5}

## Execution Flow

1. **Data Extraction**: The pipeline kicks off by invoking the `get_google_data_books` task (PythonOperator), which fetches book information (e.g., title, author, published date, category, language) from the Google Books API and pushes the results to XCom for downstream use.  
   :contentReference[oaicite:0]{index=0}

2. **Table Creation**: The `create_table` task (PostgresOperator) ensures the target `books` table exists in PostgreSQL before inserting data.

3. **Data Load**: The `insert_book_data` task (PythonOperator with PostgresHook) retrieves the cleansed dataset from XCom and performs batch insertion into the PostgreSQL `books` table.

4. **Task Scheduling & Orchestration**: Airflow’s **Scheduler** triggers the DAG based on the defined execution schedule, while the **Executor** runs tasks in order following declared dependencies. The full pipeline progression—Extract → Create Table → Load—is visually represented in Airflow’s **Graph View**.  
   :contentReference[oaicite:1]{index=1}

5. **Observability & Reliability**: Task runs, statuses, retries, and logs are tracked in Airflow’s metadata database. Using Airflow’s UI, users can access runtime logs, retry failed tasks, monitor historical runs in **Grid View**, and troubleshoot issues effectively.  
   :contentReference[oaicite:2]{index=2}

