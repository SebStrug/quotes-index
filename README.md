# Quotes I Like

Serve quotes I like with an inverted index.

There's a million quote apps out there. This is a way for me to store and serve quotes that **I like**.
Works locally and with AWS.

1. An inverted index for words in the quotes using `src.index`, run from `src.main`
1. Quotes are served to a web page via `src.api`
1. Local and AWS paths are managed by `src.handler`
1. Webpage stored in `src.static`, `src.templates`
1. Infrastructure provisioned with terraform
1. (todo) Serverless build of index
1. Pipenv for requirement management
1. Pytest for testing in `tests`

You should be able to replicate this setup for your own uses.

## Try it yourself
1. Write down some quotes in the `manual` directory in a `main.txt` file. See `manual.examples`.
1. Run `manual.split_quotes` to get the required format of a quote per enumerated file (`1.txt`, `2.txt`, ...) in a `quotes` directory. 
1. Upload to S3 with `manual.upload_quotes`