import pytest
from airflow_news.dags.news.get_news_manual import (
    ManualNewsScraper, 
    AuthorFinder
)
from unittest.mock import Mock, patch, MagicMock
import datetime
from dataclasses import dataclass
from bs4 import BeautifulSoup

module_location = "airflow_news.dags.news.get_news_manual"

@pytest.fixture
def sample_settings():
    settings = {
        "news_name": "Toronto Sun",
        "category": "politics",
        "url": "https://torontosun.com/category/news/national/federal_elections/",
        "base_url": "https://torontosun.com",
        "article_contained_in": "section",
        "attrs": {
            "class":"article-content__content-group article-content__content-group--story"
        },
        "skip_n_links": 0,
        "bias": "right",
        "use_proxy": True,
        "email_search_settings": {
            "tag": "a",
            "attrs": {
                "class": "social-share__link flex-align-justify-center",
                "title": "Send an Email"
            }
        }
    }
    return settings


@pytest.fixture
def sample_html():
    with open(
        "./test_news/fixtures/html_sample_manual_scraping.html", 
        "r", 
        encoding="utf-8"
    ) as f:
        html = f.read()
        return html

def test_init(sample_settings):
    author_finder = AuthorFinder(**sample_settings)
    assert author_finder.base_url == sample_settings['base_url']
    assert author_finder.email_search_settings == (
        sample_settings['email_search_settings']
    )

@patch(
        module_location +".AuthorFinder.get_author_email", 
        return_value="bpassifiume@postmedia.com"
)
@patch(
    module_location +".AuthorFinder._handle_author_email_not_found", 
    return_value="bpassifiume@postmedia.com"
)
def test_parse_author_info(
    mock_get_author_email, 
    mock__handle_author_email_not_found, 
    sample_html, 
    sample_settings
):
    author_finder = AuthorFinder(**sample_settings)
    html = sample_html
    bs = BeautifulSoup(html, "html.parser")
    author_text, author_url, author_email = author_finder.parse_author_info(
        bs, content=None
    )
    assert author_text == "Bryan Passifiume"
    assert author_email == "bpassifiume@postmedia.com"
    assert author_url is None