# Inverted index
Build an inverted index using a list of custom quotes

Works with a local path, as well as S3.

TODO:
1. Schedule to run every 3 days, using serverless.
1. Provide API to search and add quotes

### Details
Quotes are made from a large text file that I add to, as I read quotes that I like. I have a script (not included) that splits these up into files with one quote per file, each filename being a consecutive integer (`1.txt`, `2.txt`, ...).

I'm using this as a playground to play with pytest, serverless, terraform, pipenv, while learning about data structures.
