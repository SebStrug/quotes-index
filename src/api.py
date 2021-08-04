from pathlib import Path
from random import choices, choice
import logging
from typing import List

from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.handler import LocalHandler


local_path = Path("quotes")
index_handler = LocalHandler(local_path)
quotes_files = local_path.glob("*.txt")

inverted_index = index_handler.load_index("index")
word_id_map = index_handler.load_index("word-ids")


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
    quotes = get_all_quotes(word)
    if quotes:
        single_quote = choice(quotes)
    else:
        single_quote = "N/A"
    return templates.TemplateResponse(
        "quote.html", {"request": request, "quote": single_quote}
    )


@app.post("/quotes")
async def add_quote(quote: Quote):
    return quote.dict()


def get_all_quotes(word: str) -> List[str]:
    """Given a word, search the inverted index and retrieve
    all quotes containing that word. Case insensitive.

    Args:
        word: word to search inverted index for.

    Returns:
        list of all quotes containing given word.
    """
    word_id = word_id_map.get(word.lower())
    if not word_id:
        logging.debug(f"Word: {word} not in inverted index.")
        return []

    all_file_ids = inverted_index.get(str(word_id))

    quotes = set()
    for file_id in choices(all_file_ids, k=1):
        quote_file = next(local_path.glob(f"{file_id}*.txt"))
        with quote_file.open("r") as f:
            quote = f.read()
        quotes.add(quote)
    return list(quotes)
