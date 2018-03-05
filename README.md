# tap-text

A [Singer](https://www.singer.io/) tap for extracting data from text files.

Written for the [Stitch](https://www.stitchdata.com/) 2018 Q1 internal hackathon.

This code should not be relied upon in production systems :)

## Features

- extracts data from
  - JSONL files
  - CSV files
  - logs and other unstructured messages
- schema inferred from source data with [GenSON](https://github.com/wolverdude/GenSON)
- csv fields typed with [Pandas](https://pandas.pydata.org/)
- unstructured data parsed with [pygrok](https://github.com/garyelephant/pygrok)

## Usage

See the `example_data` directory for different configuration options.

```
pipenv install --dev
pipenv run tap-text -c example_data/json_config.json | $(pipenv --venv)/bin/singer-check-tap
```

# TODO

- Optimization. Presently the code makes a complete first pass over the input data to build a schema, but the input data may be homogeneous enough that sampling every nth row could accurately describe the structure.
- Better Grok support. Perhaps give the ability to define a set of grok patterns and define them per-directory. Also have better handling of newlines in the source logs. E.g. a stacktrace may get logged over many lines but you'd want all those lines to be part of a single log entry.
