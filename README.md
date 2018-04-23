## Usage:

  python excel.py (path_to.xlsx) [amount]

> amount - how many urls to get from *.xlsx


## Config:
```python
{
    "url_col": "column in *.xlsx with urls, <str>"
    "label_col": "column in *.xlsx with labels, <str>"
    "fetch_col": "column in *.xlsx with 1 or 0 (1 means url in that row should be fetched), <str>"
    "request_timeout": "timeout for GET request in seconds, <float>"
    "error_log_path": ...
    "log_path": ...
    "db_path": ...
    "urls_amount": "default value for amount, <int>"
}
```
