# Google Books ETL Pipeline

### DAG Execution Overview

Designed and developed an automated ETL pipeline to extract data from the Google Books API, orchestrating the process with Airflow DAGs running within Docker containers. Implemented data transformation using Pandas, standardizing the structure and format of book data for seamless integration into downstream systems.
Incorporated data quality checks to ensure the accuracy and integrity of the extracted book data before storing them in PostgreSQL.
Conducted Exploratory Data Analysis (EDA) on stored data to uncover insights and trends for decision-making.

### Monitoring via Airflow UI
- **Graph View**: Visual depiction of task dependencies and states (success, running, failed).  
- **Grid View**: Heatmap showing status of each task across multiple runs—ideal for spotting trends or failures at a glance.  
- **Task Instance View**: Clicking any task opens detailed views including logs, execution metadata, XComs, and rendered templates. This is your go-to for debugging.:contentReference[oaicite:4]{index=4}

### Logs Output
- Logs are generated per task instance and accessible directly in the UI.
- Airflow supports custom handler configurations, including local log storage and options for remote storage like S3 or GCS.:contentReference[oaicite:5]{index=5}

### Sample Execution Flow

1. **Trigger DAG** (e.g., `fetch_and_store_google_books`) via UI or CLI.
2. The **Graph View** updates to show task status transitions—from `fetch_book_data` to `create_table` to `insert_book_data`.
3. Upon successful run, each task becomes green in Grid View.
4. Click any task to inspect logs, view outputs, check XCom values, or troubleshoot failures.
5. Logs include details like API fetch responses, SQL insertion confirmations, and error stacks if something fails.

---

###  Live Example (Visual)

- **Graph View**: Shows tasks and their execution order.
- **Grid View**: Gives historical task state overview.
- **Task Logs**: Reveal run outputs and error diagnostics for each node.

*(See UI previews at the top of this README for reference)*

---

Let me know if you'd like to include sample log excerpts (e.g., API response snippets, SQL execution logs), or embed screenshots from your own local Airflow instance for authenticity.
::contentReference[oaicite:6]{index=6}
