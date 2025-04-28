import requests
from bs4 import BeautifulSoup
import os
from pathlib import Path
import time
import newspaper
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from news.utils import (check_if_can_scrape, 
    extract_article_links, get_author_email,
    verify_base_url, validate_data, correct_author_list,
    get_news_info_schema, proxy_requests
)
import re
from news.PydanticModels import NewsInfo
from datetime import timezone, datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from newspaper import Article
import time
import logging
from news.log_setup import setup_logging
import json


# Pattern for emails
unquoted_pattern = r"[\w\.-]+@[\w\-]+\.[a-zA-Z]{2,6}"

MAIN_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent

env = os.getenv('environment')
print("The environment is:", env)

if env == 'local_dev':
    MAIN_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent.parent
    setup_logging(log_file_path='./airflow_news/dags/news/logs/dalog.log')
else:
    setup_logging()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

logger = logging.getLogger("get_news")


with open(MAIN_DIR / "config/proxy_server.json", 'r') as f:
    proxies = json.load(f)
    logger.info(f"Using proxy: {proxies}")


def get_rss_feed_links(rss_link, link_attributes:dict=None) -> list:
    response = requests.get(
        rss_link, headers=headers
    )
    bs = BeautifulSoup(response.text, "xml")

    # Articles and links are contained in the item tag
    items = bs.find_all("item")

    links = []
    for item in items:
        url = item.find("link", attrs=link_attributes).text
        links.append(url)
    
    return links


class SeleniumArticleParser:
    def article(self, url:str, **kwargs) -> Article:
        """ Uses Selenium to get article info instead of beautifulsoup 
        """
        html = fetch_html(url, use_selenium=True)
        article = Article(url, browser_user_agent="Mozilla/5.0")
        article.download(input_html=html)  # Set the dynamically loaded HTML
        article.parse()
        
        return article


def fetch_html(url:str, use_selenium:bool):
    if use_selenium:
        chrome_options = Options()
        chrome_options.add_argument("--headless") 
        chrome_options.add_argument(("user-agent=Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"))
        chrome_options.add_argument("--disable-dev-shm-usage")  
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")

        # Launch Selenium WebDriver and get article 
        remote_webdriver = 'remote_chromedriver2'
        with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', 
                            options=chrome_options) as driver:

        # Uncomment for local testing    
        #driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)
            time.sleep(2)  

            html = driver.page_source
        return html
    else:
        html = requests.get(
            url, headers=headers
        ).text
    
    return html


def fetch_author_info(base_url, url:str, author:str, use_selenium:bool,
        email_search_settings=None
    ):
    
    html = fetch_html(url, use_selenium)
    bs = BeautifulSoup(html, "html.parser")
    try:
        author_url = bs.find("a", string=author).get("href")
        logger.info(f"Author_url: {author_url}")
    except AttributeError:
        logger.exception("Could not find author_url.")
        return None, None
    else:
        author_url = verify_base_url(base_url, author_url)
        html = fetch_html(author_url, use_selenium)
        
        logger.info("Attempting to find author email...")
        email = get_author_email(html, email_search_settings)

        logger.info(f"Found author {email}, {author_url}")
        return email, author_url


def get_authors(url, author_search_settings:dict, use_selenium):
    """Called if newspaper4k finds no author

    Parameters
    ----------

    author_search_settings: dict that indicates the tags and its 
        attributes where the website contains the author:
        {
            'tag' '<html_tag>',
            'attrs': {'<attribute_name>': '<attribute_value>'}
        }
    """
    attrs = author_search_settings.get("attrs")
    tag = author_search_settings.get("tag")

    # Get the html and parse
    html = fetch_html(url, use_selenium)
    bs = BeautifulSoup(html, "html.parser")

    # Get list of authors using provided settings
    authors = bs.find_all(tag, attrs=attrs)
    authors = [author.text for author in authors]
    return authors


def handle_missing_author_email(article):
    author_email = re.findall(unquoted_pattern, article.text_cleaned)
    if not author_email:
        author_email = None
    return author_email


def handle_authors(
        authors:list, link:str, author_search_settings:dict, use_selenium:bool
    ):
    """ If authors were not found, the get_authors function will be used
    to try to find the authors

    Parameters
    ----------

    authors: list list containing author names or empty list

    link:str the article URL that the authors wrote

    author_search_settings: dict formated like:

        {
            "tag": "<tag which contains the author name>",
            "attrs": {
                "<attribute_name>": "<value of attribute of which the tag has>",
                ...
            }
        }
    
    use_selenium: bool If selenium needs to be used to fetch the html
    """
    # Try to get authors if newspaper fails
    if authors == [] and author_search_settings:
        authors = get_authors(link, author_search_settings, use_selenium)
        if authors == []:
            logger.warning("No authors found")
    
    return authors


def get_article_info(
        link:str, settings:dict, article_parser
    ) -> list[dict[str, str|datetime]]:
    """ Gets the info of the linked article such as title, publish date, etc.
    If an article has more than 1 author, the article dicitonary will be added
    to the 'articles_written_by_authors' list for each author.

    Parameters:
        settings:dict that contains:
            link: str URL to get article
            base_url: str URL of the websites main home page, 
                e.g.: "https://www.website.com"
            category: str category of news
            country: str country where the outlet is from
            outlet: str should be the news outlets name with underscores replacing 
                spaces
            use_selenium (default: False): bool 
                If Selenium should be used to extract html instead of
                the requests module,
            language,
            author_search_settings (Optional)
    """
    

    bias = settings['bias']
    base_url = settings['base_url']
    category = settings['category']
    country = settings['country']
    outlet = settings['news_name']
    author_search_settings = settings.get("author_search_settings")
    use_selenium = settings.get("use_selenium", False)
    language = settings.get("language", "english")
    email_search_settings = settings.get('email_search_settings')

    schema = get_news_info_schema(
        MAIN_DIR / "config/scraper_output_schema.json"
    )
    articles_written_by_authors = []

    # Fetch article
    article = article_parser.article(
            url=link, browser_user_agent="Mozilla/5.0"
    )

    logger.debug(f"content: [{article.text_cleaned}]")
    
    authors:list = correct_author_list(article.authors, outlet)
    authors:list|None = handle_authors(
        authors, link, author_search_settings, use_selenium
    )

    logger.debug(f"Authors: {authors}")
    
    # Add the article for each author to the list of articles
    for author in authors:
        author = author.replace("\n", "").strip()
        author_email, author_url = fetch_author_info(
            base_url, link, author, use_selenium, 
            email_search_settings = email_search_settings
        )
        logger.info(f"Author email: {author_email}")
        if author_email is None:
            author_email = handle_missing_author_email(article)

        data = schema.copy()
        data.update(
            {
                "content": article.text_cleaned,
                "author": author,
                "description": article.summary,
                "publishedAt": article.publish_date.astimezone(timezone.utc),
                "title": article.title,
                "category": category,
                "url": link,
                "source": outlet,
                "country": country,
                "bias": bias,
                "author_email": author_email,
                "author_url": author_url,
                "language": language
            }
        )
        
        if validate_data(data, pydantic_model=NewsInfo):
            logger.info(f"data for {author} {link} sucessfully validated")
            articles_written_by_authors.append(data)
        else:
            logger.warning(f"data for {author} {link} failed validation")

    return articles_written_by_authors


def article_extraction_loop(
        news:list,
        links, 
        base_url,
        n_articles,
        scraper_settings,
        article_parser
    ):
    for i, link in enumerate(links):
        can_scrape = check_if_can_scrape(
            base_url=base_url, 
            url=link, 
            user_agent="Mozilla/5.0"
        )

        if can_scrape:
            logger.info(
                    f"({i+1}/{n_articles}) Getting article info from {link}"
            )
            try:
                article_info = get_article_info(
                    link, scraper_settings, article_parser
                )
                logger.info(f"Returned info:{article_info}")

                news += article_info
                time.sleep(1)
            except Exception as e:
                logger.exception(f"failed to connect to {link}")
    return news


def parse_articles(
        rss_feed_dict:dict, n_articles:int|None=None
    ) -> list[list[dict]]:
    """ Given an RSS feed link, article links will be extracted and for each 
    article, the content and meta data will be extracted with newspaper4k.

    Parameters
    ----------
    rss_feed_dict: dict dictionary containing RSS feeds for different news 
    sources. Formated like:
        {
            "news_name": "<Second-Level Domain of the base url>"
            "base_url":"<url to website main page, e.g. www.facebook.com>",
            "feeds": {
                "politics": "<rss_feed_url>"
            },
            "country": "<country>",
            "bias": "<political bias>"
        }

    
    Returns
    -------
    list
    """
    # Store all news info
    news:list[list[dict]] = []
    feed_categories = rss_feed_dict['feeds']

    use_selenium = rss_feed_dict.get("use_selenium", False)
    article_parser = newspaper if not use_selenium else SeleniumArticleParser

    with proxy_requests(proxies):
        for category in feed_categories:
            rss_url = feed_categories[category]

            logger.info(f"Getting links from {rss_url}")
            links = get_rss_feed_links(rss_url)

            logger.info("Found {} links".format(len(links)))

            n_articles = len(links) if n_articles is None else n_articles

            links = links[:n_articles]
            # Get the info for the linked articles
            news = article_extraction_loop(
                news=news,
                links=links,
                n_articles=n_articles,
                base_url=rss_feed_dict["base_url"],
                scraper_settings=rss_feed_dict,
                article_parser=article_parser
            )
    logger.info(
        f"Sucessully extracted {len(news)}/{len(links)} articles"
        )
    return news


def get_news_via_links(settings:dict, news_name:str, n_articles=None) -> list[dict]:
    """ Scrapes news by finding links in the provided URL instead of using 
    links from an RSS feed

    Parameters:
        settings:dict formated like:
            {   
                "news_name": "<Second-Level Domain of the base url>"
                "base_url":"https://www.website.com",
                "url": "https://www.website.com/<some_path>",
                "category": "<category>",
                "country": "<country>",
                "link_class": "<class name that the 'a' tag has>",
                "skip_n_links": int \\(the first skip_n_links will be ignored),
                "bias": "<bias of the news source>"
                "author_search_settings" (Optional) : {
                    "tag": "<tag which contains the author name>",
                    "attrs": {
                        "<attribute_name>": "<value of attribute of which the tag has>",
                        ...
                    },
                }
                "email_search_settings" (optional): {
                    "tag": "<tag which contains the author email>",
                    "attrs": {
                        "<attribute_name>": "<value of attribute of which the tag has>",
                        ...
                    }
                }
            }
            
        news_name:str name of the news outlet

        n_articles: int | None, if None, the function will fetch all articles 
            found
    
    Returns:
        list[dict] list containing dictionaries containing news info
    """
    news:list[dict] = []
    url = settings['url']
    base_url = settings['base_url']
    use_proxy = settings.get("use_proxy", False)

    # Selenium may need to be used insted of requests to get html
    use_selenium = settings.get("use_selenium", False)

    article_parser = newspaper if not use_selenium else SeleniumArticleParser()

    logger.info(f"Using Selenium: {use_selenium}")
    with proxy_requests(proxies, use_proxy):
        logger.info(f"Getting links from {url}")

        html = requests.get(
            url, headers=headers
        ).text

        links = extract_article_links(html, settings)

        logger.debug(f"Links: {links}")
        logger.info("Found {} links".format(len(links)))

        n_articles = len(links) if n_articles is None else n_articles
        links = links[:n_articles]
        news = article_extraction_loop(
            news=news,
            links=links,
            base_url=base_url,
            n_articles=n_articles,
            scraper_settings=settings,
            article_parser=article_parser
        )

    logger.info(
        f"Sucessully extracted {len(news)}/{len(links)} articles"
    )
    return news


if __name__ == "__main__":
    pass