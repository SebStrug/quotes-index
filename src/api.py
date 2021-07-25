from pathlib import Path
from random import choices
import logging

from fastapi import FastAPI
from pydantic import BaseModel

from src.handler import LocalHandler

local_path = Path("quotes")
index_handler = LocalHandler(local_path)
quotes_files = local_path.glob("*.txt")

inverted_index = index_handler.load_index("index")
word_id_map = index_handler.load_index("word-ids")


app = FastAPI()


class Quote(BaseModel):
    lead_in: str = None
    content: str
    attribution: str = "Anonymous"
    source: str = None


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.get("/quotes/{word}")
async def find_quote(word: str, limit: int = 1):
    word_id = word_id_map.get(word)
    logging.info(f"Word: {word}, ID: {word_id}")
    if not word_id:
        return {"quote": "N/A"}

    all_file_ids = inverted_index.get(str(word_id))
    logging.info(f"All File IDs: {all_file_ids}")

    quotes = set()
    for file_id in choices(all_file_ids, k=limit):
        quote_file = next(local_path.glob(f"{file_id}*.txt"))
        with quote_file.open("r") as f:
            quote = f.read()
            logging.info(f"Found quote: {quote}")
            quotes.add(quote)
    return [{"quote": q} for q in quotes]


@app.post("/quotes")
async def add_quote(quote: Quote):
    return quote.dict()
