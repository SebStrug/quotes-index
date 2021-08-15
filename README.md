# Quotes I Like

Serve quotes I like with an inverted index.

Check out [quotes.sebstrug.com](http://quotes.sebstrug.com).

There's a million quote apps out there. This is a way for me to store and serve quotes that **I like**.
Works locally and with AWS.

Summary:
1. An inverted index for words in the quotes using `src.index`, run from `src.main`
1. Quotes are served to a web page via `src.api`, GET & POST defined.
1. Local and AWS paths are managed by `src.handler`
1. Webpage stored in `src.static`, `src.templates`
1. Infrastructure provisioned with terraform
1. Manually set up Eventbridge rule to trigger a new index every 3 days.
1. (todo) Infrastructure-as-code for serverless build of index. Right now it's all clickety-click on AWS.

Other:
1. Pipenv for requirement management
1. Pytest for testing in `tests`

You should be able to replicate this setup for your own uses.

## Try it yourself
1. Write down some quotes in the `manual` directory in a `main.txt` file. See `manual.examples`.
1. Run `$ python -m manual.split_quotes` to get the required format of a quote per enumerated file (`1.txt`, `2.txt`, ...) in a `quotes` directory. 
1. Create local inverted index with `$ python -m src.local_index`
1. Serve local webpage with `$ uvicorn src.api:app --reload`

Want to use AWS?
1. Upload to quotes S3 with `$ python -m manual.upload_quotes`.
1. Provision Lambda function, S3 bucket, create appropriate environment variables: `QUOTES_ENV=aws; QUOTES_INDEX_S3_BUCKET=...; QUOTES_INDEX_AWS_REGION=...;`

