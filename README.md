# SQL to ElasticSearch Query DSL parser

## Installation

```bash
composer require apility/elasticsearch-sql
```

## Usage

```php
<?php

use Apility\ElasticSearch\SQLParser;

$parser = new SQLParser;
$sql = "SELECT * FROM articles WHERE published = 1 ORDER BY updated LIMIT 10";
$query = $parser->parse($sql);
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