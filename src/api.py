from typing import List, Tuple, Dict
from pathlib import Path
from random import choices, choice
import logging
import re
import os

from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import FastAPI, Request, Form, HTTPException
from cachetools import TTLCache, cached
import boto3

from src.handler import LocalHandler, AWSHandler, Handler


@cached(cache=TTLCache(maxsize=5000, ttl=3 * 60 * 60))
def get_handler() -> Handler:
    if os.getenv("QUOTES_ENV") == "aws":
        s3 = boto3.resource("s3")
        handler = AWSHandler(s3)
    else:
        local_path = Path("quotes")
        handler = LocalHandler(local_path)
    return handler


@cached(cache=TTLCache(maxsize=5000, ttl=3 * 60 * 60))
def get_indexes() -> Tuple[Dict, Dict]:
    handler = get_handler()
    inverted_index = handler.load_index("index")
    word_id_map = handler.load_index("word-ids")
    return inverted_index, word_id_map


INVERTED_INDEX, WORD_ID_MAP = get_indexes()

app = FastAPI()

app.mount("/static", StaticFiles(directory="src/static"), name="static")

templates = Jinja2Templates(directory="src/templates")


class Quote(BaseModel):
    lead_in: str = ""
    content: str
    source: str = "Anonymous"


@app.get("/", response_class=HTMLResponse)
async def serve_home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


@app.post("/", response_class=HTMLResponse)
async def search_word(request: Request, word: str = Form(...)):
    quotes = get_quotes(word)
    if not quotes:
        raise HTTPException(status_code=404, detail=f"No quote with word '{word}'")
    single_quote = choice(quotes)
    quote = split_quote(single_quote)
    return templates.TemplateResponse(
        "quote.html",
        {
            "request": request,
            "lead_in": quote.lead_in,
            "content": quote.content,
            "source": quote.source,
        },
    )


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle exceptions by returning a 404 page.

    By default FastAPI returns a simple dictionary
    """
    quote_str = get_random_quote()
    quote = split_quote(quote_str)
    return templates.TemplateResponse(
        "404.html",
        {
            "request": request,
            "lead_in": quote.lead_in,
            "content": quote.content,
            "source": quote.source,
        },
    )


@app.post("/quotes")
async def add_quote(quote: Quote):
    return quote.dict()


def get_random_quote() -> str:
    file_id = choice(list(INVERTED_INDEX.values()))
    handler = get_handler()
    return handler.load_object(str(file_id))


def get_quotes(word: str) -> List[str]:
    """Given a word, search the inverted index and retrieve
    all quotes containing that word. Case insensitive.

    Args:
        word: word to search inverted index for.

    Returns:
        list of all quotes containing given word.
    """
    word_id = WORD_ID_MAP.get(word.lower())
    if not word_id:
        logging.debug(f"Word: {word} not in inverted index.")
        return []

    all_file_ids = INVERTED_INDEX.get(str(word_id))
    handler = get_handler()

    quotes = set()
    for file_id in choices(all_file_ids, k=1):
        quote = handler.load_object(str(file_id))
        quotes.add(quote)
    return list(quotes)


def split_quote(quote: str) -> Quote:
    """Turn a quote from raw string form to Quote model class"""
    quotes_split = quote.split("\n")
    quote_dict = {}
    for ind, line in enumerate(quotes_split):
        if ind == 0 and line.endswith("..."):
            quote_dict["lead_in"] = line

        if re.match(r"\'.+\'", line, flags=re.DOTALL):
            quote_dict["content"] = line

        if ind == len(quotes_split) - 1:
            quote_dict["source"] = line

    return Quote(
        lead_in=quote_dict.get("lead_in", ""),
        content=quote_dict.get("content", ""),
        source=quote_dict.get("source", ""),
    )
