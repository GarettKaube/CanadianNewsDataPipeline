# Canadian News Data Pipeline

## Overview

News data pipeline for extracting canadian news, loading them to postgres and transforming appropriately.
The news scraping tries to use newspaper4k for majority of the weight lifting. A manual news scraper
was created to scrape what newspaper4k fails to scrape. After scraping, the news is loaded to a Postgres database and transformed with dbt core.

After, news that mention the two most prominent Canadian Prime Minister candidates: Mark Carney and Pierre Poilievre. With these articles
the OpenAI batch api is used to extract sentiment scores for each candidate if they are mentioned in the news article. These sentiment scores are
loaded to postgres and the Results are displayed using streamlit and Plotly. With this streamlit dashboard, each candidate's overal news sentiment scores
can be seen for the selected dates.

## Features

- Custom news scrapers for accurate, clean news extraction
- Uses ELT and ETL hybrid pipeline managed with Apache Airflow where ELT automates news extraction, ingestion into Postgres, dbt transformations, while french articles are extracted from postgres and translated to english, loaded back to Postgres, then dbt transformed (ETL)
- Streamlit dashboard for summarizing news sentiment for Mark Carney and Pierre Poilievre over time

## Architecture
- ### Data Ingestion
<img src="images/data_ingestion.png" width="800"/>

- ### Sentiment Analysis ETLT
<img src="images/sentiment.png" width="700"/>

## DBT models
![dbt](images/dbt.PNG)

## Streamlit Dashboard
The left plot displays the bias of the sampled news outlets the right pplot displays the average sentiment for each bias.
S slider is available to change the sample dates. News sources were selected with the goal of having an even split between left and right political views.
<img src="images/dashboard.PNG" width="900"/>
### Average Sentiment Over Time:
<img src="images/dashboard2.PNG" width="900"/>

### Word Cloud:
<img src="images/dashboard3.PNG" width="800"/>

## Tech Stack
- **Apache Airflow**: Scheduling of pipelines and automated web scraping
- **dbt core**: Transform raw tables into an analysis-ready data model
- **PostgreSQL**: Local data warehousing
- **Docker**: Containerization of the Airflow environment
- **Streamlit**: Interactive dashboard
- **OpenAI API**: Zero-shot sentiment analysis with gpt 4o mini 
- **Beautifulsoup and Selenium**: Enabled webscraping of both JavaScript and non-JavaScript websites
- **Pydantic**: Scraped/Extracted data validation


