"""fill in neo4j database with random news data."""
from .connections import Database
from .connections import Node, Relation

from typing import Optional

import random

create_node: Node = lambda type, name: Node(type, {'name': name})

sites: list[str] = [
    create_node('NewsSite', "Spiegel"),
    create_node('NewsSite', "Welt"),
    create_node('NewsSite', "YahooFinance"),
    create_node('NewsSite', "GoogleFinance")
]

componanies: list[str] = [
    create_node('Company', "BASF"),
    create_node('Company', "MSFT"),
    create_node('Company', "AMZN")
]


relationship_types: list[str] = [
    "POSITIVE",
    "NEGATIV",
    "NEUTRAL"
]


from dataclasses import dataclass
from datetime import date
from tqdm import trange


@dataclass
class Article:
    """A news article about a stock."""
    headline: str
    context: Optional[str]
    date: Optional[date]


from lorem_text import lorem

news_count: int = 200 # how much news to generate per news_site
with Database("bolt://127.0.0.1:7687", "neo4j", "123") as db:
    db.clear() # delete db

    # create sites
    [db.create_node(site) for site in sites]

    # create componanies
    [db.create_node(company) for company in componanies]

    # generate random connections/ relations from/to article from/to sites/ companies
    for news in trange(news_count):
        ## CREATE ARTICLE NODE ##
        article = Article(
            headline=lorem.words(random.randint(1, 3)).replace(" ", "")+str(random.randint(0, 1000)),
            context=lorem.paragraphs(random.randint(50, 100)),
            date=date(random.randint(2000, 2022), random.randint(1, 12), random.randint(1, 28))
        )
        db.create_node(Node(
            "Article",
            {
                "name": article.headline # only upload headline to neo4j
            }
        ))

        ## CREATE ARTICLE RELATIONS ##

        # create relation from article to company
        db.create_relationhip(
            article.headline,
            random.choice(relationship_types),
            random.choice(componanies).properties['name']
        )

        # create 'WRITTEN' reltion from site to headline
        db.create_relationhip(
            random.choice(sites).properties['name'],
            "WRITTEN",
            article.headline
        )

        # create relation from article to other companies
        [db.create_relationhip(
            article.headline,
            random.choice(relationship_types),
            random.choice(componanies).properties['name']
        ) for _ in range(random.randint(0, 5))]

    # print(db.query)
    db.commit() # commit to db
