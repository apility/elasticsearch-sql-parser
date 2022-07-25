# ElasticSearch SQL Parser

## Installation

```bash
composer require apility/elasticsearch-sql-parser
```

## Usage

```php
<?php

use Apility\ElasticSearch\SQLParser;

$query = "SELECT * FROM articles WHERE published = 1 ORDER BY updated LIMIT 10";
$parsed = SQLParser::parse($query); // ElasticSearch Query DSL
```

## Example output

```javascript
{
    "index": "articles",
    "from": 0,
    "size": 10,
    "query": {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "must": [
                            {
                                "match_phrase": {
                                    "published": {
                                        "query": "1"
                                    }
                                }
                            }
                        ]
                    }
                }
            ]
        }
    },
    "sort": [
        {
            "updated": {
                "order": "ASC"
            }
        }
    ],
    "_source": {
        "include": [
            "id",
            "name",
            "author"
        ]
    }
}
```