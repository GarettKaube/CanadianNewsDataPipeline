"""
Script containing a simple streamlit dashboard to summarize the news data
"""
import os
import pandas as pd
import datetime
import plotly.graph_objects as go
import os
import sqlalchemy as sa
import datetime
import pandas as pd
import nltk
import numpy as np
from nltk.corpus import stopwords
from  nltk.tokenize import word_tokenize
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import streamlit as st
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from airflow_news.dags.news.log_setup import setup_logging
import logging

setup_logging('./logs/sentimentdashboard.log')
logger = logging.getLogger('sentiment_dashboard')

nltk.download('stopwords')
db_address = os.getenv("POSTGRES_ADRESS")

def pull_data(n_days_ago) -> pd.DataFrame:
    engine = sa.create_engine(db_address)
    first_day = pd.Timestamp(datetime.datetime.now()) \
        - pd.DateOffset(days=n_days_ago[0])
    second_day = pd.Timestamp(datetime.datetime.now()) \
        - pd.DateOffset(days=n_days_ago[1])
    first_day = first_day.strftime("%Y-%m-%d %H:%M:%S")
    second_day = second_day.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Yesturday: {first_day}")

    query = """SELECT DISTINCT ON (a.article_id)
                    s.*, 
                    src.news_source_name, 
                    a.title,
                    src.bias,
                    a.publishedat,
                    a.url
                FROM "STAGE_DATAMART".sentiment as s
                LEFT JOIN "STAGE_DATAMART".articles as a
                    ON s.article_id = a.article_id
                LEFT JOIN "STAGE_DATAMART".sources as src
                    ON a.source_id = src.source_id
                WHERE a.publishedat BETWEEN :second_day AND :first_day
                """
    
    with engine.connect() as conn:
        result = conn.execute(
            sa.text(query), {"first_day": first_day, "second_day":second_day}
        )
        df = pd.DataFrame(result)
        logger.debug(f"Fetched df from database:\n{df}")
        
        pd.options.display.max_colwidth = 200
        if not df.empty:
            # Convert data types
            df['sentiment_mark'] = df['sentiment_mark'].astype(float)\
                .replace({0.0:np.nan})
            df['sentiment_poilievre'] = df['sentiment_poilievre'].astype(float)\
                .replace({0.0:np.nan})
            
            logger.debug(df)
            logger.debug(df['news_source_name'].value_counts())
            logger.debug(df['bias'].value_counts())
            with pd.option_context(
                'display.max_rows', 
                None, 
                'display.max_columns', 
                None, 
                'display.width', 
                None
            ):
                logger.debug("\n%s", df['url'].to_string(index=False))
                
            return df
        else:
            # Empty df
            return df



def get_relavant_article_content(n_days_ago) -> pd.DataFrame|None:
    yesterday = pd.Timestamp(datetime.datetime.now()) - pd.DateOffset(days=n_days_ago[0])
    yesterday2 = pd.Timestamp(datetime.datetime.now()) - pd.DateOffset(days=n_days_ago[1])
    meta_data = sa.MetaData()
    engine = sa.create_engine(db_address)
    table = sa.Table(
        "articles", 
        meta_data, 
        autoload_with=engine, 
        schema="STAGE_DATAMART"
    )

    with engine.connect() as conn:
        query = sa.select(
            table.c.source_id, table.c.article_content, 
            table.c.publishedat, table.c.url, table.c.article_id
        )\
            .where(table.c.publishedat.between(yesterday2, yesterday))\
            .where(table.c.article_content.ilike("%Carney%") | 
                   table.c.article_content.ilike("%Poilievre%")
            )

        result = conn.execute(query).fetchall()
        try:
            df = pd.DataFrame(result, columns=result[0].keys())
            df['article_content'] = df['article_content'].str.replace("\\n", "")

            mailto_patern = r"[\w\.-]+@[\w\-]+\.[a-zA-Z]{2,6}"

            # Remove trending now
            df['article_content'] = df['article_content'].str\
                .split("trending now").str[0]

            df['article_content'] = df['article_content'].str\
                .split("Trending Now").str[0]

            # Remove emails
            df['article_content'] = df['article_content'].str.replace(
                mailto_patern,"", regex=True
            )

            # Remove URL's
            pattern = r"www\.[a-zA-Z0-9\-]+\.[\w]{2,6}"
            df['article_content'] = df['article_content'].str.replace(
                pattern, "", regex=True
            )

            return df
        except Exception as e:
            print(e)
            return pd.DataFrame([])


sw = set(stopwords.words('english'))
sw.add("said")
sw_french = set(stopwords.words('french'))

def tokenize_article(article):
    words = word_tokenize(article)
    tokenized = [word for word in words if word not in sw and word not in sw_french and word.isalpha()]
    return tokenized


def plot_wordcloud(review, title,max_words):
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        words_filtered = tokenize_article(review)
        text = " ".join([ele for ele in words_filtered])
        word_cloud= WordCloud(
            background_color="white", 
            random_state=1,
            stopwords=sw,
            max_words=max_words,
            width=1000, 
            height=600
        )
        word_cloud.generate(text)
        fig = plt.figure(figsize=[8,15], dpi=400)
        plt.imshow(word_cloud,interpolation="bilinear")
        plt.axis('off')
        plt.title(title)
        st.pyplot(fig)
    

def plot_bias_bar_plot(
        bias_counts:pd.Series, plot_height=800, col_plot_width=500
    ):
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x = bias_counts.index,
            y = bias_counts,
            marker_color='lightskyblue',
            text=bias_counts,
            textposition='inside'
        )
    )
    
    fig.update_layout(
        xaxis_title="News Source Bias",
        yaxis_title = "Count",
        title="Number of Articles for Each Bias",
        height=plot_height,
        width=col_plot_width
    )
    return fig


def calulate_average_sentiment_for_each_bias(df:pd.DataFrame):
    df = df[["sentiment_mark", "sentiment_poilievre", "bias"]]\
        .groupby(["bias"])[["sentiment_mark", "sentiment_poilievre"]]\
        .mean()
    df = df.reset_index()
    return df


def plot_bias_per_pol(df, plot_height=800, col_plot_width=500):
    fig = go.Figure()
    # for bias in df['bias']:
    # x1 = 
    fig.add_trace(
        go.Bar(
            name='Mark Carney',
            x = df['bias'],
            y = df['sentiment_mark'],
            marker_color='Red',
            text=round(df['sentiment_mark'], 2),
            textposition='inside'
        )
    )

    fig.add_trace(
        go.Bar(
            name='Pierre Poilievre',
            x = df['bias'],
            y = df['sentiment_poilievre'],
            marker_color='Blue',
            text=round(df['sentiment_poilievre'], 2),
            textposition='inside'
        )
    )

    fig.update_layout(
        barmode='group',
        xaxis_title="News Source Bias",
        yaxis_title = "Average",
        title="Average Sentiment for Each Politician for Each News Bias",
        height=plot_height,
        width=col_plot_width
    )
    return fig


def plot_n_mentions(count_series, plot_height=800, col_plot_width=500):
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name='N mentions',
            x = count_series.index,
            y = count_series,
            marker_color='lightskyblue',
            text=count_series,
            textposition='inside'
        )
    )
    fig.update_layout(
        xaxis_title="Politician",
        yaxis_title = "Count",
        title="Number of Articles which Mention the Politician",
        height=plot_height,
        width=col_plot_width
    )
    return fig



def data_analysis(df:pd.DataFrame):
    bias_counts = df['bias'].value_counts()
    bias_per_pol = calulate_average_sentiment_for_each_bias(df)

    col = st.columns([3, 3])
    col_plot_height = 600
    col_plot_width = 800
    with col[0]:
        bias_fig = plot_bias_bar_plot(bias_counts, 
            col_plot_height, col_plot_width
        )
        st.plotly_chart(bias_fig)
    
    with col[1]:
        bias_per_pol_fig = plot_bias_per_pol(bias_per_pol, col_plot_height, 
            col_plot_width
        )
        st.plotly_chart(bias_per_pol_fig)

    count_mark = df['sentiment_mark'].dropna().shape[0]
    count_po = df['sentiment_poilievre'].dropna().shape[0]
    count_series = pd.Series(
        [count_mark, count_po], 
        index=["Mark Carney", "Pierre Poilievre"]
    )
    st.plotly_chart(plot_n_mentions(count_series, col_plot_height, 
            col_plot_width))
    
    st.plotly_chart(plot_sentiment_overtime(df))
    

def plot_sentiment_overtime(
        df:pd.DataFrame, plot_height=600, col_plot_width=2000
    ):
    df['publishedat'] = pd.to_datetime(df['publishedat'])
    df = df.set_index('publishedat')
    df = df[["sentiment_mark", "sentiment_poilievre"]]\
        .resample("D")\
        .mean()
    
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            name='sentiment_mark',
            x=df.index.astype(str),
            y = df["sentiment_mark"],
            marker_color='Red'
        )
    )
    
    fig.add_trace(
        go.Scatter(
            name='sentiment_poilievre',
            x=df.index.astype(str),
            y = df["sentiment_poilievre"],
            marker_color='Blue'
        )
    )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title = "Sentiment",
        title="Sentiment Over Time",
        height=plot_height,
        width=col_plot_width
    )
    return fig


def main():
    st.set_page_config(layout="wide")
    st.title("Mark Carney Versus Pierre Poilievre News Sentiment")
    slider = st.slider("Number of days ago", min_value=1, max_value=10, step=1,
                  value=(1,2))
    

    # Get data 
    df = pull_data(n_days_ago=slider)
    articles = get_relavant_article_content(n_days_ago=slider)

    metric_cols = st.columns([3,3,3])

    
    if not df.empty and not articles.empty:
        with metric_cols[0]:
            avg_sentiment_mark = round(df['sentiment_mark'].mean(), 3)
            std_mark = round(df['sentiment_mark'].std(), 3)
            st.metric("Overal Sentiment Mark Carney", 
                      value=f"{avg_sentiment_mark}/1.0, ({std_mark})")
            st.caption("Brackets indicate the standard deviation")

        with metric_cols[1]:
            avg_sentiment_po = round(df['sentiment_poilievre'].mean(), 3)
            std_po = round(df['sentiment_poilievre'].std(), 3)
            st.metric("Overal Sentiment Pierre Poilievre", 
                      value=f"{avg_sentiment_po}/1.0, ({std_po})")

        data_analysis(df)
    
        massive_Str = " ".join(list(articles["article_content"]))
        plot_wordcloud(massive_Str, "Word Cloud", max_words=50)


    else:
        st.write("No Data!")


if __name__ == "__main__":
    main()
