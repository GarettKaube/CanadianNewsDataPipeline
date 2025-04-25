import pytest
from airflow_news.dags.news.get_news import get_article_info
from unittest.mock import Mock, patch
import datetime
from newspaper import Article
import newspaper

module_location = 'airflow_news.dags.news.get_news'
utils_location = 'airflow_news.dags.news.utils'

@pytest.fixture
def ctv_settings():
    settings = {
        "news_name": "ctvnews",
        "category": "politics",
        "country": "ca",
        "url": "https://www.ctvnews.ca/politics/",
        "base_url": "https://www.ctvnews.ca",
        "article_contained_in": "article",
        "attrs": {
            "class":"b-article-body"
        },
        "skip_n_links": 0,
        "link_class":  "c-link",
        "use_selenium": True,
        "link_key_word": "article",
        "bias": "centre",
        "use_proxy": True,
        "email_search_settings": {
            "tag": "a",
            "attrs": {
                "class": "c-link b-full-author-bio__social-link",
                "rel":"noreferrer"
            }
        }
    }
    return settings

@pytest.fixture
def sample_html():
    with open(
        "./test_news/fixtures/html_sample.html", 
        "r", 
        encoding="utf-8"
    ) as f:
        html = f.read()
        return html
    

@pytest.fixture
def sample_author_html():
    with open(
        "./test_news/fixtures/html_author_sample.html", 
        "r", 
        encoding="utf-8"
    ) as f:
        html = f.read()
        return html


@pytest.fixture
def sample_content():
    with open(
        "./test_news/fixtures/html_sample_content.txt", 
        "r", 
    encoding="utf-8"
    ) as f:
        content = f.read()
        return content
    return content


@patch(utils_location + '.correct_author_list',
               return_value=['Stephen Hunt'])
@patch(module_location + '.handle_authors',
               return_value=['Stephen Hunt'])
@patch(module_location + '.fetch_author_info',
               return_value=(
                   "stephen.hunt@bellmedia.ca",
                    "https://www.ctvnews.ca/team/stephen-hunt/"
                )
        )
def test_get_article_info_selenium(
        mock_correct_author_list, mock_handle_authors, mock_fetch_author_inf,
        sample_html, ctv_settings, sample_content, sample_author_html
    ):
    
    class SeleniumArticleParser:
        def article(self, url:str, **kwargs) -> Article:
            """ Uses Selenium to get article info instead of beautifulsoup 
            """
            article = Article(url, browser_user_agent="Mozilla/5.0")
            article.download(input_html=sample_html)
            article.parse()
            
            return article
        

    url = ("https://www.ctvnews.ca/calgary/article"
    "       /its-disrespectful-smith-cracks-back-at-carney-comments/")

    with patch(module_location + '.fetch_html', 
               side_effect=[sample_html, sample_author_html]):
        # Get article data
        parser = SeleniumArticleParser()
        data = get_article_info(url, ctv_settings, parser)

        assert len(data) == 1
        data_dict = data[0]
        assert type(data[0]) == dict
        assert (datetime.datetime.isoformat(data_dict['publishedAt'], sep=' ') 
                == "2025-04-14 10:50:12.892000+00:00")
        
        assert data_dict['author'] == "Stephen Hunt"
        assert data_dict['title'] == ("‘It’s disrespectful’: "
                                    "Smith pushes back at Carney comments")
        # Check each word from sample is in the parsed content instead of 
        # checking for total equality due to different formating 
        for word in sample_content.replace("\n", " ").strip().split(" "):
            assert word in data_dict['content']

        assert data_dict['author_url'] == (
            "https://www.ctvnews.ca/team/stephen-hunt/"
        )
        assert data_dict['author_email'] == 'stephen.hunt@bellmedia.ca'


@pytest.fixture
def sample_html2():
    with open(
        "./test_news/fixtures/html_sample2.html", 
        "r", 
        encoding="utf-8"
    ) as f:
        html = f.read()
        return html
    

@pytest.fixture
def sample_author_html2():
    with open(
        "./test_news/fixtures/html_author_sample2.html", 
        "r", 
        encoding="utf-8"
    ) as f:
        html = f.read()
        return html


@pytest.fixture
def sample_html2_content():
    with open(
        "./test_news/fixtures/html_sample2_content.txt", 
        "r", 
        encoding="utf-8"
    ) as f:
        text = f.read()
        return text


@pytest.fixture
def globeandmail_settings():
    settings = {
        "news_name": "theglobeandmail",
        "base_url": "https://www.theglobeandmail.com",
        "link_key_word": "article",
        "url": "https://www.theglobeandmail.com/politics/",
        "category": "poltics",
        "country": "ca",
        "link_class": "c-card__grid c-card__link",
        "skip_n_links": 0,
        "bias": "centre",
        "use_proxy": True,
        "email_search_settings": {
            "tag": "a",
            "attrs": {
                "class": "o-author-meta__contact-link text-gmr-4 mb-8",
            }
        }
    }
    return settings

@patch(utils_location + '.check_if_can_scrape', return_value=True)
@patch(utils_location + '.correct_author_list',
        return_value=["Nojoud Al Mallees", "Stephanie Levitz"])
@patch(module_location + '.handle_authors',
        return_value=["Nojoud Al Mallees", "Stephanie Levitz"])
@patch(
    module_location + '.fetch_author_info',
    side_effect =[(
        'nalmallees@globeandmail.com',
        'https://www.theglobeandmail.com/authors/nojoud-al-mallees/'
    ),
    (
        'nalmallees@globeandmail.com',
        "https://www.theglobeandmail.com/authors/stephanie-levitz/"
    )
    ]
)
def test_get_article_info(
        mock_check_if_can_scrape, mock_correct_author_list, mock_handle_authors, 
        mock_fetch_author_inf,
        sample_html2, globeandmail_settings, 
        sample_html2_content, sample_author_html2
    ):
    class NewsPaperParser:
        def article(self, url:str, **kwargs) -> Article:
            """ Uses Selenium to get article info instead of beautifulsoup 
            """
            article = newspaper.article(
                url, browser_user_agent="Mozilla/5.0", input_html=sample_html2
            )
            return article

    url=("https://www.theglobeandmail.com/politics/federal-election/"
        "article-poilievre-pledges-to-repeal-ban-on-single-use-plastics/")

    # Patch fetch_html and check_if_can_scrape function
    with patch(module_location + '.fetch_html', 
               side_effect=[
                   sample_html2, sample_author_html2, 
                   sample_html2, sample_author_html2
                ]
        ) as mock_fetch:

        # Get article data
        parser = NewsPaperParser()
        data = get_article_info(url, globeandmail_settings, parser)

        # article has two authors, so the data list should have 2 article info
        # dictionaries. One for each author
        assert len(data) == 2
        data_dict1 = data[0]
        assert type(data[0]) == dict
        assert (datetime.datetime.isoformat(data_dict1['publishedAt'], sep=' ') 
                == "2025-04-18 15:13:17.111000+00:00")
        
        assert data_dict1['author'] == "Nojoud Al Mallees"
        assert data_dict1['title'] == ("As advance polls open, federal party "
                    "leaders present contrasting messages on what’s at stake")

        # Check each word from sample is in the parsed content instead of 
        # checking for total equality due to different formating 
        for word in sample_html2_content.replace("\n", " ").strip().split(" "):
            assert word in data_dict1['content']

        assert data_dict1['author_url'] == ("https://www.theglobeandmail.com"
                                               "/authors/nojoud-al-mallees/")
        assert data_dict1['author_email'] == 'nalmallees@globeandmail.com'


        data_dict2 = data[1]

        assert type(data_dict2) == dict
        assert (datetime.datetime.isoformat(data_dict2['publishedAt'], sep=' ') 
                == "2025-04-18 15:13:17.111000+00:00")
        
        assert data_dict2['author'] == "Stephanie Levitz"
        assert data_dict2['title'] == (
            "As advance polls open, federal party leaders present contrasting "
            "messages on what’s at stake"
        )

        # Check each word from sample is in the parsed content instead of 
        # checking for total equality due to different formating 
        for word in sample_html2_content.replace("\n", " ").strip().split(" "):
            assert word in data_dict2['content']

        assert data_dict2['author_url'] == (
            "https://www.theglobeandmail.com/authors/stephanie-levitz/"
        )