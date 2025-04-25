import pytest
from airflow_news.dags.news.get_news_manual import (
    ManualNewsScraper, 
    AuthorFinder
)
from unittest.mock import Mock, patch, MagicMock
import datetime
from dataclasses import dataclass
from contextlib import contextmanager
from bs4 import BeautifulSoup
import json

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


# First Param = path_to_html
# Second Param = path_to_article_content
# Third Param = dict of expected data
@pytest.fixture(params=[
    (
        "./test_news/fixtures/manual_scraper/html_sample_manual_scraping.html",
        "./test_news/fixtures/manual_scraper/html_sample_content_manual_scraping.txt",
        "./test_news/fixtures/manual_scraper/test_data1.json"
    )
])     
def sample_data(request):
    """
    First Param = path_to_html
    Second Param = path_to_article_content
    Third Param = dict of expected data
    """
    param = request.param
    with open(param[0], "r", encoding="utf-8") as f:
        html = f.read()
    
    with open(param[1], "r", encoding="utf-8") as f:
        content = f.read()

    with open(param[2], "r", encoding="utf-8") as f:
        data = json.load(f)

    return html, content, data
    

@pytest.fixture
def author_finder(sample_settings):
    return AuthorFinder(
        base_url=sample_settings['base_url'],
        email_search_settings=sample_settings.get("email_search_settings")
    )

def test_scraper_attributes(sample_settings,
                            sample_data, author_finder):
    sample_html, _, _ = sample_data
    with patch(
        "airflow_news.dags.news.get_news_manual.requests.get", 
        return_value=MagicMock(text=sample_html)
    ):
        manualscraper = ManualNewsScraper(
            sample_settings, 
            author_parser=author_finder
        )
        assert manualscraper.url == sample_settings['url']
        assert manualscraper.news_name == sample_settings['news_name']\
            .lower().replace(" ", "")
        assert manualscraper.base_url == sample_settings['base_url']
        assert manualscraper.article_contained_in == (
            sample_settings['article_contained_in']
        )
        assert manualscraper.attrs == sample_settings['attrs']
        assert manualscraper.email_search_settings == (
            sample_settings['email_search_settings']
        )

        assert manualscraper.html == sample_html


@pytest.fixture
def random_html_sample():
    """ Fake sample HTML to test _extract_article_links method
    """
    html = """
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta name="description" content="Welcome to MyCoolSite — your hub for awesome content.">
        <title>MyCoolSite</title>
        <link rel="stylesheet" href="styles.css">
    </head>
    <body>
        <header>
            <h1>MyCoolSite</h1>
            <nav>
                <ul>
                    <li><a class="article-card__link" href="https://torontosun.com/home">Home</a></li>
                    <li><a class="article-card__link" href="https://torontosun.com/about">About</a></li>
                    <li><a class="article-card__link" href="https://torontosun.com/blog">Blog</a></li>
                    <li><a class="article-card__link" href="https://torontosun.com/contact">Contact</a></li>
                </ul>
            </nav>
        </header>

        <main>
            <article>
                <h2>Welcome to MyCoolSite!</h2>
                <p>We’re excited to share the latest from our blog. Read our <a class="article-card__link" href="https://torontosun.com/blog/post-123">most recent post</a> or explore our <a class="article-card__link" href="https://torontosun.com/blog/archive">archives</a>.</p>
                <p>Looking to get involved? Visit our <a class="article-card__link" href="https://torontosun.com/community">community page</a>.</p>
            </article>

            <aside>
                <h3>Quick Links</h3>
                <ul>
                    <li><a class="article-card__link" href="https://torontosun.com/shop">Shop</a></li>
                    <li><a class="article-card__link" href="https://torontosun.com/support">Support</a></li>
                    <li><a class="article-card__link" href="https://torontosun.com/faq">FAQ</a></li>
                </ul>
            </aside>

            <section>
                <h2>Newsletter Signup</h2>
                <form action="https://torontosun.com/subscribe" method="post">
                    <label for="email">Email:</label>
                    <input type="email" id="email" name="email" required>

                    <label for="frequency">Frequency:</label>
                    <select id="frequency" name="frequency">
                        <option value="weekly">Weekly</option>
                        <option value="monthly">Monthly</option>
                    </select>

                    <button type="submit">Subscribe</button>
                </form>
            </section>
        </main>

        <footer>
            <p>&copy; 2025 MyCoolSite. All rights reserved.</p>
            <nav>
                <a class="article-card__link" href="https://torontosun.com/privacy-policy">Privacy Policy</a> |
                <a class="article-card__link" href="https://torontosun.com/terms-of-service">Terms of Service</a>
            </nav>
        </footer>
    </body>
    </html>
    """
    return html

def test_extract_article_links(
        sample_settings, sample_data, 
        random_html_sample, author_finder
    ):
    sample_html, _, _ = sample_data
    expected_urls = [
        "https://torontosun.com/home",
        "https://torontosun.com/about",
        "https://torontosun.com/blog",
        "https://torontosun.com/contact",
        "https://torontosun.com/blog/post-123",
        "https://torontosun.com/blog/archive",
        "https://torontosun.com/community",
        "https://torontosun.com/shop",
        "https://torontosun.com/support",
        "https://torontosun.com/faq",
        "https://torontosun.com/privacy-policy",
        "https://torontosun.com/terms-of-service"
    ]
    with patch(
        "airflow_news.dags.news.get_news_manual.requests.get", 
        return_value=MagicMock(text=sample_html)
    ):
        
        manualscraper = ManualNewsScraper(sample_settings, author_finder)
        manualscraper.html = random_html_sample

        urls = manualscraper._extract_article_links()

        urls.sort()
        expected_urls.sort()        
        assert urls == expected_urls


def test_parse_published_time(sample_html, sample_settings):
    bs = BeautifulSoup(sample_html)
    manualscraper = ManualNewsScraper(sample_settings, author_finder)
    pt = manualscraper._parse_published_time(bs)
    date_text = "Apr 23, 2025"
    assert pt == datetime\
        .datetime.strptime(date_text, '%b %d, %Y')


def test_parse_text(sample_settings, sample_data):
    sample_html, sample_content, _ = sample_data
    bs = BeautifulSoup(sample_html)
    manualscraper = ManualNewsScraper(sample_settings, author_finder)
    text = manualscraper._parse_text(bs)
    for word in sample_content.replace("\n", " ").strip().split(" "):
            assert word in text


def test_get_article_info(
    sample_data, sample_settings
    ):
    sample_html, sample_content, test_data = sample_data
    class AuthorFinder:
        def parse_author_info(self, *args, **kwargs):
            return [
                test_data['author_name'], 
                test_data['author_url'], 
                test_data['author_email']]
    with patch(
        "airflow_news.dags.news.get_news_manual.requests.get", 
        return_value=MagicMock(text=sample_html)
    ):
        author_finder = AuthorFinder()
        manualscraper = ManualNewsScraper(sample_settings, author_finder)
        print("RULLL:test_data['url']")
        results = manualscraper._get_article_info(test_data['url'])

        # Check extepected returned content
        for word in sample_content.replace("\n", " ").strip().split(" "):
            assert word in results['content']

        assert results['author'] == test_data['author_name']
        assert results['author_email'] == test_data['author_email']
        assert results['author_url'] == test_data['author_url']
        assert results['title'] == test_data['title']
        assert results['description'] == test_data['description']

        # Check publish time
        assert results['publishedAt'] == datetime\
            .datetime.strptime(test_data['publishedAt'], '%b %d, %Y')

@patch(
    module_location + ".ManualNewsScraper._extract_article_links",
    return_value=[("https://torontosun.com/news/national/federal_elections/"
        "federal-party-leaders-trade-jabs-as-election-heads-into-final-days")]
)     
@patch(module_location + ".proxy_requests")
def test_parse_articles(mock_proxy_requests:MagicMock, mock_extract_article_links,
            sample_html_content, sample_settings, author_finder):
        
        sample_html, sample_content, test_data = sample_html_content

        class AuthorFinder:
            def parse_author_info(self, *args, **kwargs):
                return [
                    test_data['author_name'], 
                    test_data['author_url'], 
                    test_data['author_email']]
        
        mock_proxy = MagicMock()

        mock_proxy_requests.return_value.__enter__.return_value = mock_proxy

        with patch(
            "airflow_news.dags.news.get_news_manual.requests.get", 
            return_value=MagicMock(text=sample_html)
        ):
            author_finder = AuthorFinder()
            manualscraper = ManualNewsScraper(sample_settings, author_finder)

            result = manualscraper.parse_articles(n_articles=1)

            assert len(result) == 1

            results = result[0]

            assert results['author'] == test_data['author_name']
            assert results['author_email'] == test_data['author_email']
            assert results['author_url'] == test_data['author_url']
            assert results['title'] == test_data['title']
            assert results['description'] == test_data['description']

            # Check publish time
            assert results['publishedAt'] == datetime\
                .datetime.strptime(test_data['publishedAt'], '%b %d, %Y')

    