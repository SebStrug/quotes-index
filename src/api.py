from pathlib import Path
from random import choice

from fastapi import FastAPI

from src.local import LocalIndexHandler

local_path = Path('quotes')
index_handler = LocalIndexHandler(local_path)
quotes_files = local_path.glob('*.txt')

inverted_index = index_handler.load_index('index')
word_id_map = index_handler.load_index('word-ids')


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/quotes/{word}")
def find_quote(word: str):
    word_id = word_id_map.get(word)
    print(f'Word: {word}')
    print(f'Word ID: {word_id}')
    if not word_id:
        return {'No': 'such quote'}
    file_ids = inverted_index.get(str(word_id))
    file_id = choice(file_ids)
    print(f'File IDs: {file_ids}\nFile ID: {file_id}')

    quote_file = next(local_path.glob(f'{file_id}*.txt'))
    with quote_file.open('r') as f:
        quote = f.read()
    print(f'Quote: {quote}')
    return {'quote': quote}