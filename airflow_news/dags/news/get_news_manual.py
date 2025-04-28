"""
Python script containing the ManualNewsScraper which is used when
newspaper3k/4k does not work
"""
import requests
from bs4 import BeautifulSoup
import json
import re
from requests.exceptions import RequestException, ConnectionError
import sys
import os
from pathlib import Path
from datetime import datetime
import time
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from news.utils import get_author_email as get_author_email_naive

from news.PydanticModels import NewsInfo, ManualScrapingSettings
from news.utils import (
    verify_base_url, validate_data, validate_type, get_news_info_schema,
    proxy_requests
)

env = os.getenv('environment')
print("The environment is:", env)
MAIN_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent

if env == 'local_dev':
    from news.log_setup import setup_logging
    setup_logging('./airflow_news/dags/news/logs/get_np_news.log')
    logger = logging.getLogger("get_np_news")
    MAIN_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent.parent
else:
    from news.log_setup import setup_logging
    setup_logging()
    logger = logging.getLogger("get_np_news")

# Pattern to find emails
pattern = r"^\"[\w\.-]+@[\w\-]+\.[a-zA-Z]{2,6}\""
unquoted_pattern = r"[\w\.-]+@[\w\-]+\.[a-zA-Z]{2,6}"



with open(MAIN_DIR / "config/proxy_server.json", 'r') as f:
    proxies = json.load(f)

def load_np_config(root_path:Path) -> dict:
    path = root_path / "config/newsconfig" / f"manual_scraping_config.json"
    with open(path, "r") as f:
        settings = json.load(f)
        return settings

class AuthorFinder:
    """ Class for finding an author's name, profile URL, and email
    within the article HTML or content
    """
    def __init__(self, base_url, email_search_settings:dict|None=None, **kwargs):
        """
        Parameters
        ----------
        base_url str:

        email_search_settings dict|None: dictionary formatted like:

        "email_search_settings" (optional): 
        
        {
            "tag": "tag which contains the author email",

            "attrs": {

                "attribute_name": "value of attribute of which the tag has",
                ...
            }
        }
        """
        # Extract dict settings
        self.base_url = base_url
        self.email_search_settings = email_search_settings

    def fetch_author_page_html(self, author_url):
        try:
            html = requests.get(
                author_url, headers={"User-Agent": "Mozilla/5.0"}
            ).text
            return html
        except ConnectionError:
            logger.error(f"Failed to connect to {author_url}")
            return None

    def get_author_url(self, author_html):
        """ Gets the href attribute value for the 'a' tag
        """
        try:
            author_url = author_html.find("a").get("href")
        except AttributeError:
            logger.info("Failed to get author URL")
        else:
            return author_url 

    def parse_author_info(self, bs:BeautifulSoup, content:str) -> tuple[str]:
        """ Finds the author name with beautiful soup and also trys 
        to find the authors page URL and email.

        Parameters
        ----------
        - bs: BeautifulSoup the BeautifulSoup for the article in which the
            author will be searched for

        Returns
        -------
        - author_text: str name of the author
        - author_url: str URL to the author
        - author_email: str the authors email
        """

        def handle_attribute_error(bs):
            """ Called when the author is a a company and no authors 
            were found in span tags with "class": "published-by__author"
            attributes
            """
            logger.info("Searching for company author")
            try:
                author = bs.find("div", attrs={
                        "class": "wire-published-by__company",
                        "id": "wire-company-name"
                    }
                )
                author_text = author.text
            except Exception as e:
                logger.exception(f"error finding author")

            else:
                author_url = self.get_author_url(author)
                return author_text, author_url
            
        logger.info("Getting author info...")
        author_text, author_url, author_email = None, None, None
        try:
            author = bs.find("span", attrs={"class": "published-by__author"})
            author_text = author.text
        except AttributeError as e:
            logger.error("Author not in span")
            author_text, author_url = handle_attribute_error(bs)
            logger.info(f"Found author: {author_text}")
        else:
            # Get the author's URL and email
            author_url = self.get_author_url(author)
            logger.info(f"Author URL found: {author_url}")
            author_email = self.get_author_email(author_url)
        finally:
            if author_email is None:
                logger.debug(("email None. Running "
                "self._handle_author_email_not_found"))
                author_email = self._handle_author_email_not_found(
                    author_url=author_url, 
                    author_name=author_text, content=content
                )

            logger.info(f"Email found: {author_email}")
            logger.info(f"Author found: {author_text}")
        return author_text, author_url, author_email

    def get_author_email(self, author_url:str) -> str|None:
        """ Attempts to find the authors email using regex 
        or html parsing if email_search_settings was provided in the 
        classes settings
        """
        author_email = None
        logger.info("Attempting to find author email...")
        if self.email_search_settings:
            logger.debug("Using _get_email_search_settings")
            author_email = self._get_email_search_settings(author_url)
        else:
            logger.debug("Using _search_for_author_email")
            author_email = self._search_for_author_email_regex(author_url)

        return author_email
    
    def _handle_author_email_not_found(
        self, 
        author_url,
        author_name, 
        content
    ) -> str|None:
        """ Corrects URL's to ensure the base url is contained in the authors
        page URL
        Parameters
        ----------
        - author_url: str|None URL to the authors page

        - author: str Name of author

        - content: str string of the article text
        """
        author_email = None
        
        # First try fixing URL
        if author_url:
            logger.debug(f"Email for {author_name} None, verifying base url")
            author_url = verify_base_url(self.base_url, author_url)
            logger.debug(f"New URL: {author_url}")
            author_email = self.get_author_email(author_url)
        
        # Try again to find email
        if author_email is None and author_url is not None:
            logger.debug(
                (f"Email for {author_name} None. Attempting to add https: to " 
                 "the front of the author url"
                )
            )
            if "https:" not in author_url:
                author_url = "https:" + author_url
                logger.debug(f"New URL: {author_url}")
                author_email = self.get_author_email(author_url)

            author_email = None
        
        # Try to find the email within the article itself    
        if author_email is None:
            logger.debug(("email None again. Running "
            "get_author_email_naive"))
            logger.info("Searching for email within article...")
            author_email = get_author_email_naive(content)

        return author_email
        
    def _get_email_search_settings(self, author_url)-> str|None:
        """ Uses the provided email_search_settings in the settings dict
        to find the author email within the html
        """
        html = self.fetch_author_page_html(author_url)
        bs = BeautifulSoup(html, 'html.parser')
        try:
            email = bs.find_all(
                self.email_search_settings['tag'], 
                attrs=self.email_search_settings['attrs']
            )
            email = [e.get('href') for e in email if "@" in e.get('href')]
            # Remove possible duplicate emails
            email = list(set(email))[0]
        except AttributeError:
            logger.warning(" email_search_settings failed to get author")
        except IndexError:
            logger.warning(("email_search_settings failed to get author email, list(set(email)) empty"))
        else:
            email = email.replace("mailto:", "") 
            logger.debug(f"_get_email_search_settings found email: {email}")
            return email
        
    def _search_for_author_email_regex(self, author_url:str) -> str|None:
        """ Lazy approach for finding an email in the HTML of author_url
        using regex
        """
        try:
            html = self.fetch_author_page_html(author_url)
        except RequestException as e:
            logger.error("Failed to get author")
        else:
            try:
                email = re.search(unquoted_pattern, html).group()
            except AttributeError:
                logger.info(f"{author_url} has no email")
            else:
                logger.debug(f"_search_for_author_email found email: {email}")
                return email
    

class ManualNewsScraper:
    """
    """
    def __init__(self, settings:dict, author_parser):
        """
        Expects a settings dict of format:
            {
                "news_name": "<name>",
                "category": "<category>",
                "url": "<url>",
                "base_url": "<base_url>",
                "article_contained_in": "<html_tag_with_article_content>",
                "attrs": \\
                    {"<html_tag_attribute_name  which article content is \\
                        contained in>":"<value>"},
                "skip_n_links": <int>,
                "links_attr": "<>"
                "email_search_settings" (optional): {
                    "tag": "<tag which contains the author email>",
                    "attrs": {
                        "<attribute_name>": "<value of attribute of which the tag has>",
                        ...
                    }
                }
            }

        Note, this is currently designed for the National Post and Toronto Sun
        """
        # Validate parameter data type
        settings = ManualScrapingSettings(**settings)
        
        self.settings:dict = settings

        # Extract dict settings
        self.url = settings.url
        self.news_name = settings.news_name.lower().replace(" ", "")
        self.base_url = settings.base_url
        self.article_contained_in = settings.article_contained_in
        self.attrs = settings.attrs.model_dump(by_alias=True)
        try: 
            self.email_search_settings = (
                settings.email_search_settings.model_dump()
            )
        except AttributeError:
             self.email_search_settings = None

        # The attribute in the 'a' tag which contains the article links
        default_link_attr = {"class":"article-card__link"}
        #self.links_attr = self.settings.get("links_attr", default_link_attr)
        self.links_attr = settings.links_attr
        # Get HTML of the provided URL
        response = requests.get(
            self.url, headers={"User-Agent": "Mozilla/5.0"}
        )
        self.html = response.text

        self.author_parser = author_parser

        #self.skip_n_links: number of links to skip at the start of the URL list. 
        # Example use:  toronto sun has 5 trending stories at the top 
        # of the page, hence we skip these.
        self.skip_n_links = settings.skip_n_links

    def _extract_article_links(self) -> list:
        """ Searches for article links in 'a' html tags
        Parameters:
            skip_n_links: number of links to skip at the start of the URL list.
                Example use:  toronto sun has 5 trending stories at the top 
                of the page, hence we skip these.
        Returns:
         list that contains the links to the articles
        """
        bs = BeautifulSoup(self.html, "html.parser")
        articles = bs.find_all("a", attrs=self.links_attr)
        # Aricle links are stored in the href attribute for the 'a' tags
        urls = [art.get("href") for art in articles[self.skip_n_links:]]
        
        # URL's for example in toronto sun, start with /news/...
        # instead of https://torontosun.com
        if "https" not in urls[0]:
            urls = [self.base_url + url for url in urls]
            if self.base_url in urls:
                urls.remove(self.base_url)

        return urls
    
    
    def _parse_text(self, article_bs:BeautifulSoup) -> str:
        """ Searches for the contents of the article 
        """
        article_section = article_bs.find_all(
            self.article_contained_in, attrs=self.attrs
        )
        # Text is most likely split in multiple paragraphs so all p tags are 
        # found with there respective text and stored in text list
        text = []
        for section in article_section:
            try:
                section_text = section.find_all("p")
                section_text = [t.text for t in section_text]
            except AttributeError:
                logger.error((f"failed to find paragraphs for text. dtype:" 
                             f"{type(section_text)}"))
            else:
                text.extend(section_text)
        
        text_joined = " ".join(text)
        return text_joined
    
    def _parse_description(self, bs:BeautifulSoup) -> str|None:
        try:
            desc = bs.find("p", attrs={"class": "article-subtitle"})
            desc = desc.text
        except AttributeError:
            logger.info(f"desc is {type(desc)}: No description found")
        else:
            return desc

    def _parse_title(self, bs:BeautifulSoup) -> str|None:
        """ Searches for the title of the article
        Parameters
        ----------
        - bs: BeautifulSoup the BeautifulSoup for the article
        """
        try:
            title_html = bs.find("h1", attrs={
                    "id":"articleTitle",
                    "class": "article-title",
                }
            )
            title = title_html.text
            return title
        except AttributeError:
            logger.error(f"title_html.text did not work.")
            return None

    def _parse_published_time(self, bs) -> str|None:
        """
        Parameters
        ----------
        - bs: BeautifulSoup the BeautifulSoup for the article
        """
        def has_class_containing(tag):
            return tag.has_attr("class") and any(
                "published-date__since" in c for c in tag["class"]
            )
        
        try:
            date_html = bs.find(has_class_containing)
            date_text = date_html.text\
                .replace("Published", "")\
                .strip()
            date = datetime.strptime(date_text, '%b %d, %Y')
        except AttributeError:
            logger.exception(f"Error finding publish time:")
            return None
        else:
            return date

    def _get_article_info(self, url) -> dict:
        """
        Parameters
        ----------
        - url: str the url to the article
        """
        response = requests.get(
            url, headers={"User-Agent": "Mozilla/5.0"}
        )
        bs = BeautifulSoup(response.text, "html.parser")

        try:
            text = self._parse_text(bs)
            author, author_url, email = self.author_parser.parse_author_info(
                bs, text
            )
            
            description = self._parse_description(bs)
            time = self._parse_published_time(bs)
            title = self._parse_title(bs)
        except Exception as e:
            logger.exception(f"error finding parsing url:{url}")
        else:
            return {
                "content": text,
                "author": author,
                "author_url": author_url,
                "author_email": email,
                "description": description,
                "publishedAt": time,
                "title": title,
            }
        

    def parse_articles(self, n_articles:int=10) -> list[dict]:
        """ Extracts article links from the settings URL and uses
        beautiful soup to parse the links content and metadata
        """
        validate_type(n_articles, int, "n_articles")

        # REMINDER: change  load_to_postgres the newsairflowtasks 
        # if an item in the schema is removed/added
        schema = get_news_info_schema(
            MAIN_DIR / "config/scraper_output_schema.json"
        )
        
        with proxy_requests(proxies):
            # Get URLs
            urls = self._extract_article_links()[:n_articles]

            # Extract content and metadata for each url found
            news_articles = []
            for i, url in enumerate(urls):
                logger.info(
                        f"({i+1}/{n_articles}) Getting article info from {url}"
                )
                info = self._get_article_info(url)
                if info:
                    info["url"] = url
                    info['category'] = self.settings.category
                    info['source'] = self.settings.news_name
                    info['country'] = self.settings.country
                    info['bias'] = self.settings.bias
                    info['language'] = self.settings.language
                    
                    data = schema.copy()
                    data.update(info)
                    
                    if validate_data(data, pydantic_model=NewsInfo):
                        news_articles.append(data)

                    time.sleep(2)

        logger.info(
            f"Sucessully extracted {len(news_articles)}/{len(urls)} articles"
        )
        return news_articles
    

def main():
    # settings = {
    #     "news_name": "Toronto Sun",
    #     "category": "politics",
    #     "url": "https://torontosun.com/category/news/national/federal_elections/",
    #     "base_url": "https://torontosun.com",
    #     "article_contained_in": "section",
    #     "attrs": {
    #         "class":"article-content__content-group article-content__content-group--story"
    #     },
    #     "skip_n_links": 5,
    #     "bias": "right",
    #     "use_proxy": True,
    #     "email_search_settings": {
    #         "tag": "a",
    #         "attrs": {
    #             "class": "social-share__link flex-align-justify-center",
    #             "title": "Send an Email"
    #         }
    #     }
    # }

    settings = {
        "news_name": "National Post",
        "category": "politics",
        "url": "https://nationalpost.com/category/news/politics/",
        "base_url": "https://nationalpost.com",
        "article_contained_in": "section",
        "attrs": {
            "class":"story-v2-content-element article-content__content-group article-content__content-group--story"
        },
        "skip_n_links": 5,
        "bias": "right",
        "use_proxy": True
    }

    auth_parser = AuthorFinder(**settings)
    scrap = ManualNewsScraper(settings, auth_parser)
    print(scrap.parse_articles(n_articles=10))


if __name__ == "__main__":
    main()