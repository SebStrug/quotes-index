from pathlib import Path
import itertools

COUNTER = itertools.count()


def main():
    Path('quotes').mkdir(parents=True, exist_ok=True)

    quote = ''
    fname = Path(__file__).parent / 'main.txt'
    with open(fname, 'r') as f:
        for line in f.readlines():
            quote += line

            if quote.strip() == '':
                continue

            if line == '\n':
                fname = Path('quotes') / f'{next(COUNTER)}.txt'
                with open(fname, 'w') as f:
                    f.write(quote.strip())
                quote = ''


if __name__ == '__main__':
    main()
