# tasks : 1) fetch google data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
# operators : Python Operator and PostgresOperator
# hooks - allows connection to postgres
# dependencies

from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_google_data_books(num_books, ti):
    base_url = "https://www.googleapis.com/books/v1/volumes"

    params = {
        'q': 'Fantasy or Action',  # search term
        'maxResults': num_books,  # number of books
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        books_data = response.json()
        books = []
        for item in books_data.get('items', []):
            title = item['volumeInfo'].get('title', 'No Title')
            authors = item['volumeInfo'].get('authors', ['Unknown'])
            publishedDate = item['volumeInfo'].get('publishedDate')
            category = item['volumeInfo'].get('categories', ['Unknown'])[0]
            language = item['volumeInfo'].get('language')

            books.append({
                'Title': title,
                'Author': ', '.join(authors),
                'PublishedDate': publishedDate,
                'Category': category,
                'Language': language,
            })

    else:
        print(f"Failed to retrieve data: Status code {response.status_code}")
        return []

    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(books)

    # Push the DataFrame to XCom
    ti.xcom_push(key='book_data', value=df.to_dict('records'))



# 3) create and store data in table on postgres (load)

def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (title, authors, publishedDate,category,language)
    VALUES (%s, %s, %s, %s,%s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['PublishedDate'], book['Category'],book['Language']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_google_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Google and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

# operators : Python Operator and PostgresOperator
# hooks - allows connection to postgres


fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_google_data_books,
    op_args=[30],  # Number of books to fetch
    dag=dag,
)



create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
        CREATE TABLE if not exists books (
            title TEXT NOT NULL,
            authors TEXT,
            publishedDate TEXT,
            category TEXT,
            language TEXT
        );""",
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

# dependencies

fetch_book_data_task >> create_table_task >> insert_book_data_task
