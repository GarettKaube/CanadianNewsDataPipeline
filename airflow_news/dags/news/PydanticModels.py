""" Script containing a Pydantic data class for the extracted news data
"""

from pydantic import BaseModel, EmailStr, field_validator, Field
from datetime import datetime
import re
from typing import Optional, Literal

class NewsInfo(BaseModel):
    content:  str | None = None
    author: str | None = None
    description:str | None = None
    publishedAt: datetime
    title: str
    category: str
    url: str
    source: str
    country: str
    author_email: EmailStr|None
    author_url: str|None

    @field_validator('author', mode='before')
    @classmethod
    def name_must_be_valid(cls, v):
        if v is None:
            return None
        if not re.match(r"^[A-Za-zÀ-ÖØ-öø-ÿ'\.+ -]+$", v):
            return None
        return v

class ArticleContainedinAttrs(BaseModel):
    class_: str = Field(..., alias='class')

class EmailSearchSettings(BaseModel):
    tag: str
    attrs: dict

class ManualScrapingSettings(BaseModel):
    news_name: str
    category: str
    url: str
    base_url: str
    article_contained_in: str
    attrs: ArticleContainedinAttrs
    bias: Literal['left', 'right', 'centre']
    use_proxy: bool = False
    links_attr: dict = {"class":"article-card__link"}
    country: str = 'ca'
    language: str = 'english'
    skip_n_links: Optional[int] = None
    email_search_settings: Optional[EmailSearchSettings] = None
        
class SentimentDC(BaseModel):
    sentiment_mark: float | None = None
    sentiment_poilievre: float | None = None