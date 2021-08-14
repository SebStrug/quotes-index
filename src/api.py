from typing import List, Tuple, Dict
from pathlib import Path
from random import choices, choice
import logging
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
    lead_in: str = None
    content: str
    attribution: str = "Anonymous"
    source: str = None


@app.get("/", response_class=HTMLResponse)
async def serve_home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


@app.post("/", response_class=HTMLResponse)
async def search_word(request: Request, word: str = Form(...)):
    quotes = get_quotes(word)
    if not quotes:
        raise HTTPException(
            status_code=404, detail=f"No quote with word '{word}'")
    single_quote = choice(quotes)
    return templates.TemplateResponse(
        "quote.html", {"request": request, "quote": single_quote}
    )


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request, exc):
    quote = get_random_quote()
    return templates.TemplateResponse("404.html", {"request": request, "quote": quote})


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
