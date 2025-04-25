from airflow_news.dags.news.utils import (
    extract_article_links, 
    correct_author_list,
    get_author_email
)
import pytest

@pytest.fixture
def html_sample():
    html = """<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>MyCoolSite Links</title>
    </head>
    <body>
        <h1>Explore MyCoolSite</h1>
        <ul>
            <li><a href="https://mycoolsite.net/home">Home</a></li>
            <li><a href="https://mycoolsite.net/tech/trends-2025">Tech Trends 2025</a></li>
            <li><a href="https://mycoolsite.net/deals/spring-sale">Spring Sale</a></li>
            <li><a href="https://mycoolsite.net/api/v1/users/42">User API</a></li>
            <li><a href="https://mycoolsite.net/forum/thread/12345">Forum Thread</a></li>
            <li><a href="https://mycoolsite.net/news/world">World News</a></li>
        </ul>
    </body>
    </html>"""
    return html

@pytest.fixture
def complex_html():
    html = """<html lang="en">
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
                    <li><a href="https://mycoolsite.net/home">Home</a></li>
                    <li><a href="https://mycoolsite.net/about">About</a></li>
                    <li><a href="https://mycoolsite.net/blog">Blog</a></li>
                    <li><a href="https://mycoolsite.net/contact">Contact</a></li>
                </ul>
            </nav>
        </header>

        <main>
            <article>
                <h2>Welcome to MyCoolSite!</h2>
                <p>We’re excited to share the latest from our blog. Read our <a href="https://mycoolsite.net/blog/post-123">most recent post</a> or explore our <a href="https://mycoolsite.net/blog/archive">archives</a>.</p>
                <p>Looking to get involved? Visit our <a href="https://mycoolsite.net/community">community page</a>.</p>
            </article>

            <aside>
                <h3>Quick Links</h3>
                <ul>
                    <li><a href="https://mycoolsite.net/shop">Shop</a></li>
                    <li><a href="https://mycoolsite.net/support">Support</a></li>
                    <li><a href="https://mycoolsite.net/faq">FAQ</a></li>
                </ul>
            </aside>

            <section>
                <h2>Newsletter Signup</h2>
                <form action="https://mycoolsite.net/subscribe" method="post">
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
                <a href="https://mycoolsite.net/privacy-policy">Privacy Policy</a> |
                <a href="https://mycoolsite.net/terms-of-service">Terms of Service</a>
            </nav>
        </footer>
    </body>
    </html>
    """
    return html


def test_extract_article_links(html_sample):
    settings = {
        "base_url": "https://mycoolsite.net/"
    }

    urls = extract_article_links(html_sample, settings)

    assert urls != []

    expected_urls = [
        "https://mycoolsite.net/home",
        "https://mycoolsite.net/tech/trends-2025",
        "https://mycoolsite.net/deals/spring-sale",
        "https://mycoolsite.net/api/v1/users/42",
        "https://mycoolsite.net/forum/thread/12345",
        "https://mycoolsite.net/news/world"
    ]   

    for url in expected_urls:
        assert url in urls, f"{url} not in urls"


def test_extract_article_links_complex_html(complex_html):
    expected_urls = [
        "https://mycoolsite.net/home",
        "https://mycoolsite.net/about",
        "https://mycoolsite.net/blog",
        "https://mycoolsite.net/contact",
        "https://mycoolsite.net/blog/post-123",
        "https://mycoolsite.net/blog/archive",
        "https://mycoolsite.net/community",
        "https://mycoolsite.net/shop",
        "https://mycoolsite.net/support",
        "https://mycoolsite.net/faq",
        "https://mycoolsite.net/privacy-policy",
        "https://mycoolsite.net/terms-of-service"
    ]

    settings = {
        "base_url": "https://mycoolsite.net/"
    }

    urls = extract_article_links(complex_html, settings)

    assert urls != []

    for url in expected_urls:
        assert url in urls, f"{url} not in urls"


def test_correct_author_list():
    outlet = "CTV_news"
    author = ["Billy Bob", "CTV news"]
    fixed = correct_author_list(author, outlet)

    assert fixed != []
    assert type(fixed[0]) == str
    assert fixed == ["Billy Bob"]


def test_correct_author_list_website():
    outlet = "CTV_news"
    author = ["Billy Bob", "www.website.com/shop"]
    fixed = correct_author_list(author, outlet)
    
    assert fixed != []
    assert type(fixed[0]) == str
    assert fixed == ["Billy Bob"]
    

@pytest.fixture
def sample_html_with_author_email():
    str_email1 = """
        jgkosjfksjfaw fjksarje htjkehj
        kajka jdak emailname@gmail.com, faiujhfa
        iheriohfjioe
        adaoidawjdoiaj569034
    """

    str_email2 = """
        <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Email Example</title>
    </head>
    <body>
        <h1>Contact Us</h1>
        <p>If you have any questions, feel free to reach out to us at: 
            <a href="mailto:support@mycoolsite.net">support@mycoolsite.net</a>
        </p>
    </body>
    </html>
    """
    return str_email1, str_email2


def test_get_author_email(sample_html_with_author_email):
    str_email1, str_email2 = sample_html_with_author_email
    email = get_author_email(str_email1)
    assert email == "emailname@gmail.com"


def test_get_author_email_html(sample_html_with_author_email):
    str_email1, str_email2 = sample_html_with_author_email
    email = get_author_email(str_email2)
    assert email == "support@mycoolsite.net"