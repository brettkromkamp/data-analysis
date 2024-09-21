# DAG (Directed Acyclic Graph)
# Components:
#   Tasks
#       1. Fetch Amazon data (extract)
#       2. Clean data (transform)
#       3. Load data in to Postgres (load)
#   Operators
#       1. PythonOperator
#       2. PostgresOperator
#   Hooks
#       1. PostgresHook
#   Dependencies

import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from bs4 import BeautifulSoup


# region Functions
def get_amazon_books_data(num_books, ti):
    # ----- Extract -----

    # Base URL of the Amazon search results for data science books
    base_url = f"https://www.amazon.com/s?k=data+engineering+books"

    books = []
    seen_titles = set()  # To keep track of seen titles

    page = 1

    while len(books) < num_books:
        url = f"{base_url}&page={page}"

        # Send a request to the URL
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the content of the request with BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")

            # Find book containers (you may need to adjust the class names based on the actual HTML structure)
            book_containers = soup.find_all("div", {"class": "s-result-item"})

            # Loop through the book containers and extract data
            for book in book_containers:
                title = book.find("span", {"class": "a-text-normal"})
                author = book.find("a", {"class": "a-size-base"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})

                if title and author and price and rating:
                    book_title = title.text.strip()

                    # Check if title has been seen before
                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append(
                            {
                                "Title": book_title,
                                "Author": author.text.strip(),
                                "Price": price.text.strip(),
                                "Rating": rating.text.strip(),
                            }
                        )

            # Increment the page number for the next iteration
            page += 1
        else:
            print("Failed to retrieve the page")
            break

    # ----- Transform -----

    # Limit to the requested number of books
    books = books[:num_books]

    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(books)

    # Remove duplicates based on 'Title' column
    df.drop_duplicates(subset="Title", inplace=True)

    # Remove superfluous characters from the 'Price' column
    df["Price"] = df["Price"].str.replace("$", "").str.replace(".", "")

    # Push the DataFrame to XCom
    ti.xcom_push(key="book_data", value=df.to_dict("records"))


# ----- Load -----


def insert_amazon_books_data(ti):
    book_data = ti.xcom_pull(key="book_data", task_ids="fetch_books_data")
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id="amazon_books")
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book["Title"], book["Author"], book["Price"], book["Rating"]))


# endregion

# region Setup
headers = {
    "Referer": "https://www.amazon.com/",
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    "User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

logger = logging.getLogger("airflow.task")
# endregion

# DAG and tasks
dag = DAG(
    "fetch_and_load_amazon_data",
    default_args=default_args,
    description="A DAG to fetch and load Amazon book data",
    schedule_interval=timedelta(days=1),
)

# Tasks

fetch_books_data_task = PythonOperator(
    task_id="fetch_books_data",
    python_callable=get_amazon_books_data,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)
create_books_table_task = PostgresOperator(
    task_id="create_books_table",
    postgres_conn_id="amazon_books",
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)
insert_books_data_task = PythonOperator(
    task_id="insert_books_data",
    python_callable=insert_amazon_books_data,
    dag=dag,
)

# Dependency graph
fetch_books_data_task >> create_books_table_task >> insert_books_data_task

# endregion
