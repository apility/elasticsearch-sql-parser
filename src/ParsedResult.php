<?php

namespace Apility\ElasticSearch;

class ParsedResult
{
    public function __construct(protected array $parsed)
    {
        //
    }

    public function fields(): array
    {
        return $this->parsed['_source']['include'] ?? ['*'];
    }

    public function size(): int
    {
        return $this->parsed['size'] ?? SQLParser::MAX_SIZE;
    }

    public function from(): int
    {
        return $this->parsed['from'] ?? 0;
    }

    public function indices(): array
    {
        return $this->parsed['indices'] ?? [];
    }

    public function index(): string
    {
        return implode(',', $this->indices());
    }

    public function query(): array
    {
        return $this->parsed['query'] ?? [];
    }

    public function body(): array
    {
        $body = $this->parsed;

        if (array_key_exists('indices', $body)) {
            unset($body['indices']);
        }

        return $body;
    }
}
