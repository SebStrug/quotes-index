from pathlib import Path
from random import choice
import logging

from fastapi import FastAPI

from src.handler import LocalIndexHandler

local_path = Path("quotes")
index_handler = LocalIndexHandler(local_path)
quotes_files = local_path.glob("*.txt")

inverted_index = index_handler.load_index("index")
word_id_map = index_handler.load_index("word-ids")


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/quotes/{word}")
def find_quote(word: str):
    word_id = word_id_map.get(word)
    logging.info(f"Word: {word}")
    logging.info(f"Word ID: {word_id}")
    if not word_id:
        return {"quote": "N/A"}
    file_ids = inverted_index.get(str(word_id))
    file_id = choice(file_ids)
    logging.info(f"File IDs: {file_ids}\nFile ID: {file_id}")

    quote_file = next(local_path.glob(f"{file_id}*.txt"))
    with quote_file.open("r") as f:
        quote = f.read()
    logging.info(f"Quote: {quote}")
    return {"quote": quote}
