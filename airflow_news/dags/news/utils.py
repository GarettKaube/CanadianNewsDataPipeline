import urllib.request
import urllib.robotparser
from bs4 import BeautifulSoup
import re
from pydantic import ValidationError
import json
from pydantic import BaseModel
import requests
from contextlib import contextmanager
import os
from pathlib import Path
from datetime import timezone, datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def check_if_can_scrape(base_url:str, url:str, user_agent:str):
    """
    base_url: url where '/robots.txt' will follow
    url: the url we want to check in the robots.txt if we can scrape
    """
    rp = urllib.robotparser.RobotFileParser()

    req = urllib.request.Request(
        base_url + "/robots.txt", headers={"User-Agent": "Mozilla/5.0"}
    )

    with urllib.request.urlopen(req) as response:
        robots_txt = response.read().decode("utf-8")

    rp.parse(robots_txt.split("\n"))

    return rp.can_fetch(user_agent, url)


def extract_article_links(html:str, settings:dict) -> list:
    """ Searches for article links in 'a' html tags
    Parameters:
        settings: dict that must contain: 
            {
                "url": "https://www.website.com/<some_path>",
                "link_class": "<class name that the 'a' tag has>",
                "skip_n_links": int \\(the first skip_n_links will be ignored)
            }
    Returns:
        list that contains the links to the articles
    """
    base_url = settings['base_url']
    class_ = settings.get('link_class')
    skip_n_links = settings.get("skip_n_links", 0)

    # URL's may contain a key word such as "article" indicating the URL 
    # is an article
    link_key_word = settings.get('link_key_word')

    bs = BeautifulSoup(html, "html.parser")
    articles = bs.find_all("a", attrs={"class":class_})
    # Aricle links are stored in the href attribute for the 'a' tags
    urls = [art.get("href") for art in articles[skip_n_links:]]
    
    # URL's for example in toronto sun, start with /news/...
    # instead of https://torontosun.com
    expected = base_url
    for i, url in enumerate(urls):
        if expected not in url:
            urls[i] = expected + url

    if link_key_word:
        urls = [url for url in urls if link_key_word in url]

    # Remove duplicate links
    urls = list(dict.fromkeys(urls))

    return urls


def correct_author_list(authors:list, outlet) -> list:
    """ Deals with the issue of newspaper4k thinking the news outlet beside the 
    authors name is another author. 
    Sometimes articles have the news outlet as the only author so we consider
    that the author
    newspaper4k also sometimes thinks a url near the author names 
    is an author so remove it
    """
    pattern=r'[^a-zA-Z0-9\s]'
    
    if len(authors) > 1:
        outlet_prefix:str = outlet.split("_")[0].lower()
        for author in authors:
            if ((outlet_prefix in author.lower()) or 
                re.search(pattern, author)
            ):
                authors.remove(author)
    
    return authors


    
def get_author_email(text:str, search_settings:dict=None) -> str|None:
    """ Searches text for an email and returns the email.
    If no email is found, None is returned
    """
    if search_settings:
        bs = BeautifulSoup(text, 'html.parser')
        try:
            email = bs.find_all(
                search_settings['tag'], attrs=search_settings['attrs']
            )
            email = [e.get('href') for e in email if "@" in e.get('href')]
            # Remove possible duplicate emails
            email = list(set(email))[0]
        except AttributeError:
            print("No text attribute.")
        
        except IndexError:
            return None
        
        else:
            email = email.replace("mailto:", "") 
            return email
    
    else:
        email_pattern = r"[\w\.-]+@[\w\-]+\.[a-zA-Z]{2,6}"
        try:
            email = list(set(re.findall(email_pattern, text)))
        except AttributeError:
            pass
        else:
            # Remove any editor emails
            try:
                if len(email) > 1:
                    for e in email:
                        if 'editor' in e:
                            email.remove(e)
            
                return email[0]
            except IndexError as e:
                return None
    

def verify_base_url(base_url:str, url:str) -> str:
    """ Checks if base_url is in url. If not, base_url is concatenated at the
    front of url.
    """
    if base_url not in url:
        url = base_url + url
    return url


def validate_data(data:dict, pydantic_model:BaseModel) -> dict|str:
    try:
        # Validate data types 
        pydantic_model(**data)
    except ValidationError as e:
        print(e)
        return None
    else:
        return data
    

def validate_type(data, expected_type, param_name:str):
    if not isinstance(data, expected_type):
        raise TypeError(
            (f"Expect '{param_name}' to be type '{expected_type}', "
                f"got: {type(data)} instead"
            )
        )
    

def get_news_info_schema(path):
    """ Loads the json file which contains the schema of the 
    outputed data from the news scrapers
    """
    with open(path, 'r') as f:
        schema = json.load(f)
        return schema


@contextmanager
def proxy_requests(proxies, use_proxy = False):
    """ Context manager to change behavior of request.get to use a proxy
    """
    original_get = requests.get
    if use_proxy:
        requests.get = lambda *args, **kwargs: original_get(
            *args, proxies=proxies, **kwargs
        )
    else:
        requests.get = original_get
    try:
        yield
    finally:
        requests.get = original_get




