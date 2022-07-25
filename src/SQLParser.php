<?php

namespace Apility\ElasticSearch;

use PHPSQLParser\Options;
use PHPSQLParser\processors\DefaultProcessor;

class SQLParser
{
    protected array $ast = [];
    protected array $query = [];

    protected int $topHits = 0;

    protected array $aggregations = [];
    protected array $hasAggregrations = [];

    protected array $sort = [];
    protected $indices = [];

    protected string $version = '5.x';
    protected string $firstGroup = '';

    protected array $limit = [
        'from' => 0,
        'size' => 0,
    ];

    protected int $count_tmp = 0;
    protected int $count_tmp_filter = 0;
    protected int $count_tmp_range = 0;
    protected int $count_fi = 0;
    protected int $count_tmp_have = 0;
    protected int $count_tmp_filter_have = 0;
    protected int $count_tmp_range_have = 0;
    protected int $count_fi_have = 0;

    protected $arrtmp = [];

    protected string $tmp_str = '';
    protected string $tmp_str_filter = '';
    protected string $tmp_fi = '';
    protected string $tmp_str_range = '';
    protected string $tmp_lock = '';
    protected string $tmp_lock_str = '';

    protected int $tmp_or = 0;
    protected int $tmp_and = 0;

    protected string $tmp_lock_fi = '';
    protected string $tmp_lock_range = '';
    protected string $tmp_str_have = '';
    protected string $tmp_str_filter_have = '';
    protected string $tmp_fi_have = '';
    protected string $tmp_str_range_have = '';
    protected string $tmp_lock_have = '';
    protected string $tmp_lock_str_have = '';
    protected string $tmp_lock_fi_have = '';
    protected string $tmp_lock_range_have = '';

    protected final function __construct(array $config = ['version' => '5.x'])
    {
        $this->version = $config['version'] ?? '5.x';
    }

    public static function parse(string $sql, array $config = ['version' => '5.x']): array
    {
        return (new static($config))->build($sql);
    }

    protected function build($sql): array
    {
        $processor = new DefaultProcessor(new Options([]));
        $this->ast = $processor->process($sql);

        if (isset($this->ast['FROM']) && !empty($this->ast['FROM'])) {
            $this->table($this->ast['FROM']);
        }

        if (isset($this->ast['LIMIT']) && !empty($this->ast['LIMIT'])) {
            $this->limit($this->ast['LIMIT']);
            if (isset($this->ast['GROUP']) && !empty($this->ast['GROUP'])) {
                $this->query['size'] = 0;
                $this->limit($this->ast['LIMIT']);
            } else {
                $this->query['from'] = $this->limit['from'] * $this->limit['size'];
                $this->query['size'] = $this->limit['size'];
            }
        } else {
            $this->limit([]);
        }

        if (isset($this->ast['HAVING']) && !empty($this->ast['HAVING'])) {
            $this->having($this->ast['HAVING']);
        }

        if (isset($this->ast['WHERE']) && !empty($this->ast['WHERE'])) {
            $this->where($this->ast['WHERE']);
        }

        if (isset($this->ast['GROUP']) && !empty($this->ast['GROUP'])) {
            $this->groupBy($this->ast['GROUP']);
            if (!empty($this->aggregations['aggs'])) {
                $this->query['aggs'] = $this->aggregations['aggs'];
            }
        }

        if (isset($this->ast['ORDER']) && !empty($this->ast['ORDER'])) {
            $this->orderBy($this->ast['ORDER']);
            if (!empty($this->sort['sort'])) {
                $this->query['sort'] = $this->sort['sort'];
            }
        }

        if (isset($this->ast['SELECT']) && !empty($this->ast['SELECT'])) {
            $this->select($this->ast['SELECT']);
        }

        if (!isset($this->query) && empty($this->query)) {
            $this->query['query']['match_all'] = (object)[];
        }

        return $this->query;
    }

    protected function table($arr)
    {
        foreach ($arr as $v) {
            if ($v['table']) {
                $this->indices[] = $v['table'];
            }
        }

        $this->indices = array_unique($this->indices);
        $this->query['index'] = implode(',', $this->indices);
    }

    protected function where($arr)
    {
        for ($i = 0; $i < count($arr); $i++) {
            if ($arr[$i]['expr_type'] === 'bracket_expression') {
                if ($arr[$i]['sub_tree']) {
                    if (count($arr[$i]['sub_tree']) > 1) {
                        if (isset($arr[$i]['sub_tree'][0]['expr_type']) && $arr[$i]['sub_tree'][0]['expr_type'] === 'bracket_expression') {
                            for ($jj = 0; $jj < count($arr[$i]['sub_tree']); $jj++) {
                                $this->whereOr($arr[$i]['sub_tree'], $jj);
                            }
                        } else {
                            $tmp_arr = $arr[$i]['sub_tree'];
                            for ($j = 0; $j < count($tmp_arr); $j++) {
                                $this->whereExt($tmp_arr, $j);
                            }
                        }
                    } else {
                        if (isset($arr[$i]['sub_tree'][0]['expr_type']) && $arr[$i]['sub_tree'][0]['expr_type'] === 'bracket_expression') {
                            $tmp_arr = $arr[$i]['sub_tree'][0]['sub_tree'];
                        } else {
                            $tmp_arr = $arr[$i]['sub_tree'];
                        }
                        for ($j = 0; $j < count($tmp_arr); $j++) {
                            $this->whereExt($tmp_arr, $j);
                        }
                    }
                }
            } else {
                $this->whereExt($arr, $i);
            }
        }
    }

    protected function whereOrExt($arr): array
    {
        $tmp_or = [];

        for ($i = 0; $i < count($arr); $i++) {
            if (!is_numeric($arr[$i]['base_expr'])) {
                $lowerstr = strtolower($arr[$i]['base_expr']);
            } else {
                $lowerstr = $arr[$i]['base_expr'];
            }

            switch ($lowerstr) {
                case '=':
                    if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                        break;
                    }

                    if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'and' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'and') {
                        if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                            $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                            if ($term_tmp_arr[1] !== 'keyword') {
                                $termk = $term_tmp_arr[1];
                            } else {
                                $termk = $arr[$i - 1]['base_expr'];
                            }
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }

                        $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                        $tmp_date_str = str_replace("'", "", $tmp_date_str);

                        if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                            $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                            $tmp_or['bool']['must'][] = $term;
                        } else {
                            $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                            $tmp_or['bool']['must'][] = $term;
                        }
                    } else {
                        if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                            $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                            if ($term_tmp_arr[1] !== 'keyword') {
                                $termk = $term_tmp_arr[1];
                            } else {
                                $termk = $arr[$i - 1]['base_expr'];
                            }
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }

                        $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                        $tmp_date_str = str_replace("'", "", $tmp_date_str);

                        if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                            $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                            $tmp_or['bool']['must'][] = $term;
                        } else {
                            $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                            $tmp_or['bool']['must'][] = $term;
                        }
                    }
                    break;
                case '!==':
                    if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                        break;
                    }

                    if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'and' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'and') {
                        if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                            $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                            if ($term_tmp_arr[1] !== 'keyword') {
                                $termk = $term_tmp_arr[1];
                            } else {
                                $termk = $arr[$i - 1]['base_expr'];
                            }
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }

                        $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                        $tmp_date_str = str_replace("'", "", $tmp_date_str);

                        if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                            $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                            $tmp_or['bool']['must_not'][] = $term;
                        } else {
                            $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                            $tmp_or['bool']['must_not'][] = $term;
                        }
                    } else {
                        if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                            $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                            if ($term_tmp_arr[1] !== 'keyword') {
                                $termk = $term_tmp_arr[1];
                            } else {
                                $termk = $arr[$i - 1]['base_expr'];
                            }
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }

                        $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                        $tmp_date_str = str_replace("'", "", $tmp_date_str);

                        if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                            $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                            $tmp_or['bool']['must_not'][] = $term;
                        } else {
                            $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                            $tmp_or['bool']['must_not'][] = $term;
                        }
                    }
                    break;
                case '<>':
                    if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                        break;
                    }
                    if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'and' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'and') {
                        if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                            $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                            if ($term_tmp_arr[1] !== 'keyword') {
                                $termk = $term_tmp_arr[1];
                            } else {
                                $termk = $arr[$i - 1]['base_expr'];
                            }
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }

                        $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                        $tmp_date_str = str_replace("'", "", $tmp_date_str);

                        if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                            $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                            $tmp_or['bool']['must_not'][] = $term;
                        } else {
                            $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                            $tmp_or['bool']['must_not'][] = $term;
                        }
                    } else {
                        if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                            $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                            if ($term_tmp_arr[1] !== 'keyword') {
                                $termk = $term_tmp_arr[1];
                            } else {
                                $termk = $arr[$i - 1]['base_expr'];
                            }
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }

                        $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                        $tmp_date_str = str_replace("'", "", $tmp_date_str);

                        if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                            $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                            $tmp_or['bool']['must_not'][] = $term;
                        } else {
                            $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                            $tmp_or['bool']['must_not'][] = $term;
                        }
                    }
                    break;
                case 'in':
                    if (strtolower($arr[$i - 1]['base_expr']) === 'not') {
                        break;
                    }

                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    if (isset($arr[$i + 1]['sub_tree']) && !empty($arr[$i + 1]['sub_tree'])) {
                        foreach ($arr[$i + 1]['sub_tree'] as &$vv) {
                            if (!is_numeric($vv['base_expr']) && $this->version === '8.x') {
                                $termk .= '.keyword';
                            }

                            $tmp_date_str = str_replace('"', '', $vv['base_expr']);
                            $tmp_date_str = str_replace("'", "", $tmp_date_str);
                            $tmp_or['terms'][$termk][] = $tmp_date_str;
                        }
                    }

                    break;
                case 'not':
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            if ($term_tmp_arr[1] !== 'keyword') {
                                $termk = $term_tmp_arr[1];
                            } else {
                                $termk = $arr[$i - 1]['base_expr'];
                            }
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    switch (strtolower($arr[$i + 1]['base_expr'])) {
                        case 'in':
                            if (isset($arr[$i + 2]['sub_tree']) && !empty($arr[$i + 2]['sub_tree'])) {
                                foreach ($arr[$i + 2]['sub_tree'] as &$vv) {
                                    if (!is_numeric($vv['base_expr']) && $this->version === '8.x') {
                                        $termk .= '.keyword';
                                    }

                                    $tmp_date_str = str_replace('"', '', $vv['base_expr']);
                                    $tmp_date_str = str_replace("'", "", $tmp_date_str);
                                    $tmp_or['bool']['must_not']['terms'][$termk][] = $tmp_date_str;
                                }
                            }
                            break;
                        case 'like':
                            $tmp_la_str = str_replace('"', '', $arr[$i + 2]['base_expr']);
                            $tmp_la_str = str_replace("'", "", $tmp_la_str);

                            if (!is_numeric($arr[$i + 2]['base_expr']) && $this->version === '8.x') {
                                $term['wildcard'][$termk . '.keyword'] = str_replace("%", "*", $tmp_la_str);
                                $tmp_or['bool']['must_not'][] = $term;
                            } else {
                                $term['wildcard'][$termk] = str_replace("%", "*", $tmp_la_str);
                                $tmp_or['bool']['must_not'][] = $term;
                            }
                            break;
                        case 'null':
                            $tmp_or['exists']['field'] = $arr[$i - 2]['base_expr'];
                            break;
                    }

                    break;
                case 'is':
                    if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                        break;
                    }

                    if (strtolower($arr[$i + 1]['base_expr']) === 'not') {
                        break;
                    }

                    $tmp_or['bool']['must_not'][]['exists']['field'] = $arr[$i - 1]['base_expr'];

                    break;
                case '>':
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);
                    $is_date = strtotime($tmp_date_str) ? strtotime($tmp_date_str) : false;
                    $tmp_or['range'][$termk]['gt'] = $tmp_date_str;

                    if (!isset($tmp_or['range'][$termk]['time_zone']) && $is_date) {
                        $tmp_or['range'][$termk]['time_zone'] = "+08:00";
                    }

                    break;
                case '>=':
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);
                    $is_date = strtotime($tmp_date_str) ? strtotime($tmp_date_str) : false;
                    $tmp_or['range'][$termk]['gte'] = $tmp_date_str;

                    if (!isset($tmp_or['range'][$termk]['time_zone']) && $is_date) {
                        $tmp_or['range'][$termk]['time_zone'] = "+08:00";
                    }

                    break;
                case '<':
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);
                    $is_date = strtotime($tmp_date_str) ? strtotime($tmp_date_str) : false;
                    $tmp_or['range'][$termk]['lt'] = $tmp_date_str;

                    if (!isset($tmp_or['range'][$termk]['time_zone']) && $is_date) {
                        $tmp_or['range'][$termk]['time_zone'] = "+08:00";
                    }

                    break;
                case '<=':
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);
                    $is_date = strtotime($tmp_date_str) ? strtotime($tmp_date_str) : false;
                    $tmp_or['range'][$termk]['lte'] = $tmp_date_str;

                    if (!isset($tmp_or['range'][$termk]['time_zone']) && $is_date) {
                        $tmp_or['range'][$termk]['time_zone'] = "+08:00";
                    }
                    break;
                case 'like':
                    if (strtolower($arr[$i - 1]['base_expr']) === 'not') {
                        break;
                    }

                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_la_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_la_str = str_replace("'", "", $tmp_la_str);

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['wildcard'][$termk . '.keyword'] = str_replace("%", "*", $tmp_la_str);
                        $tmp_or['bool']['must'][] = $term;
                    } else {
                        $term['wildcard'][$termk] = str_replace("%", "*", $tmp_la_str);
                        $tmp_or['bool']['must'][] = $term;
                    }

                    break;
                case 'between':
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);
                    $is_date = strtotime($tmp_date_str) ? strtotime($tmp_date_str) : false;
                    $tmp_or['range'][$termk]['gte'] = $tmp_date_str;

                    if (!isset($tmp_or['range'][$termk]['time_zone']) && $is_date) {
                        $tmp_or['range'][$termk]['time_zone'] = "+08:00";
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 3]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);
                    $tmp_or['range'][$termk]['lte'] = $tmp_date_str;

                    break;
            }
        }

        return $tmp_or;
    }

    protected function whereOrInk(array $arr, int $i): array
    {
        $tmparrs = $arr;

        if (isset($tmparrs[$i]['base_expr']) && strtolower($tmparrs[$i]['base_expr']) !== 'or') {
            $this->arrtmp[] = $arr[$i];
            $i = $i + 1;
            $this->whereOrInk($tmparrs, $i);
        }

        return $this->arrtmp;
    }

    protected function whereOr(array $arr, int $i)
    {
        if (!is_numeric($arr[$i]['base_expr'])) {
            $lowerstr = strtolower($arr[$i]['base_expr']);
        } else {
            $lowerstr = $arr[$i]['base_expr'];
        }

        switch ($lowerstr) {
            case 'or':
                if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                    if ($this->tmp_str_filter === '' && !$this->tmp_or) {
                        $this->count_tmp_filter++;
                    }
                } else if ($this->tmp_str !== '') {
                    $this->count_tmp_filter++;
                }

                if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str !== $lowerstr) {
                    if ($this->tmp_str === '') {
                        $this->count_tmp++;
                    }
                }

                if (!isset($arr[$i - 2])) {
                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]['bool']['should'][] = $this->whereOrExt($arr[$i - 1]['sub_tree']);
                }

                if ($arr[$i + 1]['expr_type'] === 'bracket_expression') {
                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]['bool']['should'][] = $this->whereOrExt($arr[$i + 1]['sub_tree']);
                } else {
                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]['bool']['should'][] = $this->whereOrExt($this->whereOrInk($arr, $i + 1));
                    $this->arrtmp = [];
                }

                $this->tmp_or = 1;

                break;
            case 'and':
                if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                    if ($this->tmp_str_filter === '' && !$this->tmp_and) {
                        $this->count_tmp_filter++;
                    } else if ($this->tmp_str_filter !== '') {
                        $this->count_tmp_filter++;
                    }
                } else if ($this->tmp_str !== '') {
                    $this->count_tmp_filter++;
                }

                if (!isset($arr[$i - 2])) {
                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][] = $this->whereOrExt($arr[$i - 1]['sub_tree']);
                }

                if ($arr[$i + 1]['expr_type'] === 'bracket_expression') {
                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][] = $this->whereOrExt($arr[$i + 1]['sub_tree']);
                } else {
                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][] = $this->whereOrExt($this->whereOrInk($arr, $i + 1));
                    $this->arrtmp = [];
                }

                $this->tmp_and = 1;

                break;
        }
    }

    protected function whereExt(array $arr, int $i)
    {
        if (!is_numeric($arr[$i]['base_expr'])) {
            $lowerstr = strtolower($arr[$i]['base_expr']);
        } else {
            $lowerstr = $arr[$i]['base_expr'];
        }

        switch ($lowerstr) {
            case '=':
                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str !== $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                            $this->count_tmp++;
                        }
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_tmp]['bool']['should'][] = $term;
                    } else {
                        $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_tmp]['bool']['should'][] = $term;
                    }

                    unset($term['match_phrase']);

                    $this->tmp_lock = $lowerstr;
                    $this->tmp_lock_str = $lowerstr;
                } else if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'and' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'and') {
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str !== $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                            $this->count_tmp++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][] = $term;
                    } else {
                        $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][] = $term;
                    }

                    unset($term['match_phrase']);

                    $this->tmp_lock_str = $lowerstr;
                } else {
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][] = $term;
                    } else {
                        $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][] = $term;
                    }

                    unset($term['match_phrase']);

                    $this->tmp_lock_str = $lowerstr;
                }

                $this->tmp_lock = $lowerstr;
                $this->tmp_str = $lowerstr;

                break;
            case '!==':
                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][0]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str !== $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                            $this->count_tmp++;
                        }
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][$this->count_tmp]['bool']['should'][] = $term;
                    } else {
                        $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][$this->count_tmp]['bool']['should'][] = $term;
                    }

                    unset($term['match_phrase']);

                    $this->tmp_lock = $lowerstr;
                    $this->tmp_lock_str = $lowerstr;
                } else if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'and' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'and') {
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str !== $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                            $this->count_tmp++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][] = $term;
                    } else {
                        $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][] = $term;
                    }

                    unset($term['match_phrase']);

                    $this->tmp_lock_str = $lowerstr;
                } else {
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][] = $term;
                    } else {
                        $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][] = $term;
                    }

                    unset($term['match_phrase']);

                    $this->tmp_lock_str = $lowerstr;
                }

                $this->tmp_lock = $lowerstr;
                $this->tmp_str = $lowerstr;

                break;
            case '<>':
                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][0]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str !== $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                            $this->count_tmp++;
                        }
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][$this->count_tmp]['bool']['should'][] = $term;
                    } else {
                        $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][$this->count_tmp]['bool']['should'][] = $term;
                    }

                    unset($term['match_phrase']);

                    $this->tmp_lock = $lowerstr;
                    $this->tmp_lock_str = $lowerstr;
                } else if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'and' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'and') {
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str !== $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                            $this->count_tmp++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][] = $term;
                    } else {
                        $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][] = $term;
                    }

                    unset($term['match_phrase']);

                    $this->tmp_lock_str = $lowerstr;
                } else {
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_date_str = str_replace("'", "", $tmp_date_str);

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][] = $term;
                    } else {
                        $term['match_phrase'][$termk]['query'] = $tmp_date_str;
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][] = $term;
                    }
                    unset($term['match_phrase']);
                    $this->tmp_lock_str = $lowerstr;
                }

                $this->tmp_lock = $lowerstr;
                $this->tmp_str = $lowerstr;

                break;
            case 'in':
                if (strtolower($arr[$i - 1]['base_expr']) === 'not') {
                    break;
                }

                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                    if ($term_tmp_arr[1] !== 'keyword') {
                        $termk = $term_tmp_arr[1];
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock === $lowerstr) {
                    if ($this->tmp_str_filter === '') {
                        $this->count_tmp_filter++;
                    } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                        $this->count_tmp_filter++;
                    }
                } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                    $this->count_tmp_filter++;
                }

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (isset($this->query['query']['bool']['filter']['bool']['should'][$this->count_tmp]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str === $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                            $this->count_tmp++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp++;
                    }

                    if (isset($arr[$i + 1]['sub_tree']) && !empty($arr[$i + 1]['sub_tree'])) {
                        foreach ($arr[$i + 1]['sub_tree'] as &$vv) {
                            if (!is_numeric($vv['base_expr']) && $this->version === '8.x') {
                                $termk .= '.keyword';
                            }

                            $tmp_date_str = str_replace('"', '', $vv['base_expr']);
                            $tmp_date_str = str_replace("'", "", $tmp_date_str);

                            $this->query['query']['bool']['filter']['bool']['should'][$this->count_tmp]['terms'][$termk][] = $tmp_date_str;
                        }
                    }
                } else {
                    if (isset($arr[$i + 1]['sub_tree']) && !empty($arr[$i + 1]['sub_tree'])) {
                        if ($this->version === '7.x') {
                            $this->count_tmp_filter++;
                        }

                        foreach ($arr[$i + 1]['sub_tree'] as &$vv) {
                            if (!is_numeric($vv['base_expr']) && $this->version === '8.x') {
                                $termk .= '.keyword';
                            }

                            $tmp_date_str = str_replace('"', '', $vv['base_expr']);
                            $tmp_date_str = str_replace("'", "", $tmp_date_str);
                            $this->query['query']['bool']['filter'][$this->count_tmp_filter]['terms'][$termk][] = $tmp_date_str;
                        }
                    }
                }

                $this->tmp_lock = $lowerstr;
                $this->tmp_str = $termk;

                unset($termk);

                break;
            case 'not':
                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                    if ($term_tmp_arr[1] !== 'keyword') {
                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock === $lowerstr) {
                    if ($this->tmp_str_filter === '') {
                        $this->count_tmp_filter++;
                    } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                        $this->count_tmp_filter++;
                    }
                } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                    $this->count_tmp_filter++;
                }

                switch (strtolower($arr[$i + 1]['base_expr'])) {
                    case 'in':
                        if (isset($arr[$i + 2]['sub_tree']) && !empty($arr[$i + 2]['sub_tree'])) {
                            foreach ($arr[$i + 2]['sub_tree'] as &$vv) {
                                if (!is_numeric($vv['base_expr']) && $this->version === '8.x') {
                                    $termk .= '.keyword';
                                }

                                $tmp_date_str = str_replace('"', '', $vv['base_expr']);
                                $tmp_date_str = str_replace("'", "", $tmp_date_str);
                                $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not']['terms'][$termk][] = $tmp_date_str;
                            }
                        }

                        break;

                    case 'like':
                        $tmp_la_str = str_replace('"', '', $arr[$i + 2]['base_expr']);
                        $tmp_la_str = str_replace("'", "", $tmp_la_str);

                        if (!is_numeric($arr[$i + 2]['base_expr']) && $this->version === '8.x') {
                            $term['wildcard'][$termk . '.keyword'] = str_replace("%", "*", $tmp_la_str);
                            $this->query['query']['filter'][$this->count_tmp_filter]['bool']['must_not'][] = $term;
                        } else {
                            $term['wildcard'][$termk] = str_replace("%", "*", $tmp_la_str);
                            $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][] = $term;
                        }

                        break;
                    case 'null':
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['exists']['field'] = $arr[$i - 2]['base_expr'];
                        break;
                }

                $this->tmp_lock = $lowerstr;
                $this->tmp_str = $termk;

                unset($termk);

                break;
            case 'is':
                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (strtolower($arr[$i + 1]['base_expr']) === 'not') {
                    break;
                }

                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                    if ($term_tmp_arr[1] !== 'keyword') {
                        if ($term_tmp_arr[1] !== 'keyword') {
                            $termk = $term_tmp_arr[1];
                        } else {
                            $termk = $arr[$i - 1]['base_expr'];
                        }
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock === $lowerstr) {
                    if ($this->tmp_str_filter === '') {
                        $this->count_tmp_filter++;
                    } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                        $this->count_tmp_filter++;
                    }
                } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                    $this->count_tmp_filter++;
                }

                $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must_not'][]['exists']['field'] = $arr[$i - 1]['base_expr'];

                $this->tmp_lock = $lowerstr;
                $this->tmp_str = $termk;

                unset($termk);

                break;
            case '>':
                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);

                    if ($term_tmp_arr[1] !== 'keyword') {
                        $termk = $term_tmp_arr[1];
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_date_str = str_replace("'", "", $tmp_date_str);
                $is_date = strtotime($tmp_date_str) ? strtotime($tmp_date_str) : false;

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (isset($this->query['query']['bool']['filter'][0]) && $this->tmp_lock !== '' && $this->tmp_lock === $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_fi !== '' && $this->tmp_lock_fi === $lowerstr) {
                        if ($this->tmp_fi === '') {
                            $this->count_fi++;
                        } else if ($this->tmp_fi !== '' && $this->tmp_fi !== $termk) {
                            $this->count_fi++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][0]) && $this->tmp_lock_range !== '') {
                        if ($this->tmp_str_range === '') {
                            $this->count_tmp_range++;
                        } else if ($this->tmp_str_range !== '' && $this->tmp_str_range !== $termk) {
                            $this->count_tmp_range++;
                        }
                    }

                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['gt'] = $tmp_date_str;

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']) && $is_date) {
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone'] = "+08:00";
                    }
                } else {
                    if (isset($this->query['query']['bool']['filter'][0]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str !== $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                            $this->count_tmp++;
                        }
                    }

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['range']) && $this->tmp_lock !== '') {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['gt'] = $tmp_date_str;

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']) && $is_date) {
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone'] = "+08:00";
                    }
                }

                $this->tmp_str = $termk;
                $this->tmp_lock_str = $lowerstr;
                $this->tmp_lock = $lowerstr;
                $this->tmp_lock_range = $lowerstr;
                $this->tmp_lock_fi = $lowerstr;

                break;
            case '>=':
                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    if ($term_tmp_arr[1] !== 'keyword') {
                        $termk = $term_tmp_arr[1];
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_date_str = str_replace("'", "", $tmp_date_str);
                $is_date = strtotime($tmp_date_str) ? strtotime($tmp_date_str) : false;

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (isset($this->query['query']['bool']['filter'][0]) && $this->tmp_lock !== '' && $this->tmp_lock === $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_fi !== '' && $this->tmp_lock_fi === $lowerstr) {
                        if ($this->tmp_fi === '') {
                            $this->count_fi++;
                        } else if ($this->tmp_fi !== '' && $this->tmp_fi !== $termk) {
                            $this->count_fi++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][0]) && $this->tmp_lock_range !== '') {
                        if ($this->tmp_str_range === '') {
                            $this->count_tmp_range++;
                        } else if ($this->tmp_str_range !== '' && $this->tmp_str_range !== $termk) {
                            $this->count_tmp_range++;
                        }
                    }

                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['gte'] = $tmp_date_str;

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']) && $is_date) {
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone'] = "+08:00";
                    }
                } else {
                    if (isset($this->query['query']['bool']['filter'][0]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str !== $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                            $this->count_tmp++;
                        }
                    }

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['range']) && $this->tmp_lock !== '') {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['gte'] = $tmp_date_str;

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']) && $is_date) {
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone'] = "+08:00";
                    }
                }

                $this->tmp_str = $termk;
                $this->tmp_lock_str = $lowerstr;
                $this->tmp_lock = $lowerstr;
                $this->tmp_lock_range = $lowerstr;
                $this->tmp_lock_fi = $lowerstr;

                break;
            case '<':
                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    if ($term_tmp_arr[1] !== 'keyword') {
                        $termk = $term_tmp_arr[1];
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_date_str = str_replace("'", "", $tmp_date_str);
                $is_date = strtotime($tmp_date_str) ? strtotime($tmp_date_str) : false;

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (isset($this->query['query']['bool']['filter'][0]) && $this->tmp_lock !== '' && $this->tmp_lock === $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_fi !== '' && $this->tmp_lock_fi === $lowerstr) {
                        if ($this->tmp_fi === '') {
                            $this->count_fi++;
                        } else if ($this->tmp_fi !== '' && $this->tmp_fi !== $termk) {
                            $this->count_fi++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][0]) && $this->tmp_lock_range !== '') {
                        if ($this->tmp_str_range === '') {
                            $this->count_tmp_range++;
                        } else if ($this->tmp_str_range !== '' && $this->tmp_str_range !== $termk) {
                            $this->count_tmp_range++;
                        }
                    }

                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['lt'] = $tmp_date_str;

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']) && $is_date) {
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone'] = "+08:00";
                    }
                } else {
                    if (isset($this->query['query']['bool']['filter'][0]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str === $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        }
                    }

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['range']) && $this->tmp_lock !== '') {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['lt'] = $tmp_date_str;

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']) && $is_date) {
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone'] = "+08:00";
                    }
                }

                $this->tmp_str = $termk;
                $this->tmp_lock_str = $lowerstr;
                $this->tmp_lock = $lowerstr;
                $this->tmp_lock_range = $lowerstr;
                $this->tmp_lock_fi = $lowerstr;

                break;
            case '<=':
                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    if ($term_tmp_arr[1] !== 'keyword') {
                        $termk = $term_tmp_arr[1];
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_date_str = str_replace("'", "", $tmp_date_str);
                $is_date = strtotime($tmp_date_str) ? strtotime($tmp_date_str) : false;

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (isset($this->query['query']['bool']['filter'][0]) && $this->tmp_lock !== '' && $this->tmp_lock === $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_fi !== '' && $this->tmp_lock_fi === $lowerstr) {
                        if ($this->tmp_fi === '') {
                            $this->count_fi++;
                        } else if ($this->tmp_fi !== '' && $this->tmp_fi !== $termk) {
                            $this->count_fi++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][0]) && $this->tmp_lock_range !== '') {
                        if ($this->tmp_str_range === '') {
                            $this->count_tmp_range++;
                        } else if ($this->tmp_str_range !== '' && $this->tmp_str_range !== $termk) {
                            $this->count_tmp_range++;
                        }
                    }

                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['lte'] = $tmp_date_str;

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone']) && $is_date) {
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][$this->count_tmp_range]['range'][$termk]['time_zone'] = "+08:00";
                    }
                } else {
                    if (isset($this->query['query']['bool']['filter'][0]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str === $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                            $this->count_tmp++;
                        }
                    }

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['range']) && $this->tmp_lock !== '') {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp_filter++;
                    }

                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['lte'] = $tmp_date_str;

                    if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']) && $is_date) {
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone'] = "+08:00";
                    }
                }

                $this->tmp_str = $termk;
                $this->tmp_lock_str = $lowerstr;
                $this->tmp_lock = $lowerstr;
                $this->tmp_lock_range = $lowerstr;
                $this->tmp_lock_fi = $lowerstr;

                break;
            case 'like':
                if (strtolower($arr[$i - 1]['base_expr']) === 'not') {
                    break;
                }

                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    if ($term_tmp_arr[1] !== 'keyword') {
                        $termk = $term_tmp_arr[1];
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                $tmp_la_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_la_str = str_replace("'", "", $tmp_la_str);

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (isset($this->query['query']['bool']['filter'][0]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][0]) && $this->tmp_lock_fi !== '' && $this->tmp_lock_fi !== $lowerstr) {
                        if ($this->tmp_fi === '') {
                            $this->count_fi++;
                        } else if ($this->tmp_fi !== '' && $this->tmp_fi !== $termk) {
                            $this->count_fi++;
                        }
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['wildcard'][$termk . '.keyword'] = str_replace("%", "*", $tmp_la_str);
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][] = $term;
                    } else {
                        $term['wildcard'][$termk] = str_replace("%", "*", $tmp_la_str);
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][$this->count_fi]['bool']['should'][] = $term;
                    }
                } else {
                    if (isset($this->query['query']['bool']['filter'][0]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str !== $lowerstr) {
                        if ($this->tmp_str === '') {
                            $this->count_tmp++;
                        } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                            $this->count_tmp++;
                        }
                    }

                    if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                        if ($this->tmp_str_filter === '') {
                            $this->count_tmp_filter++;
                        } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                            $this->count_tmp_filter++;
                        }
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['wildcard'][$termk . '.keyword'] = str_replace("%", "*", $tmp_la_str);
                        $this->query['query']['filter'][$this->count_tmp_filter]['must'][$this->count_tmp]['bool']['must'][] = $term;
                    } else {
                        $term['wildcard'][$termk] = str_replace("%", "*", $tmp_la_str);
                        $this->query['query']['bool']['filter'][$this->count_tmp_filter]['bool']['must'][] = $term;
                    }
                }

                unset($term['wildcard']);

                $this->tmp_lock_str = $lowerstr;
                $this->tmp_lock = $lowerstr;
                $this->tmp_lock_fi = $lowerstr;

                break;
            case 'between':
                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }

                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    if ($term_tmp_arr[1] !== 'keyword') {
                        $termk = $term_tmp_arr[1];
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                if (isset($this->query['query']['bool']['filter'][0]) && $this->tmp_lock_str !== '' && $this->tmp_lock_str !== $lowerstr) {
                    if ($this->tmp_str === '') {
                        $this->count_tmp++;
                    } else if ($this->tmp_str !== '' && $this->tmp_str !== $termk) {
                        $this->count_tmp++;
                    }
                }

                if (isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]) && $this->tmp_lock !== '' && $this->tmp_lock !== $lowerstr) {
                    if ($this->tmp_str_filter === '') {
                        $this->count_tmp_filter++;
                    } else if ($this->tmp_str_filter !== '' && $this->tmp_str_filter !== $termk) {
                        $this->count_tmp_filter++;
                    }
                }

                $tmp_date_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_date_str = str_replace("'", "", $tmp_date_str);
                $is_date = strtotime($tmp_date_str) ? strtotime($tmp_date_str) : false;

                $this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['gte'] = $tmp_date_str;

                if (!isset($this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone']) && $is_date) {
                    $this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['time_zone'] = "+08:00";
                }

                $tmp_date_str = str_replace('"', '', $arr[$i + 3]['base_expr']);
                $tmp_date_str = str_replace("'", "", $tmp_date_str);

                $this->query['query']['bool']['filter'][$this->count_tmp_filter]['range'][$termk]['lte'] = $tmp_date_str;

                $this->tmp_str = $termk;
                $this->tmp_lock_str = $lowerstr;
                $this->tmp_lock = $lowerstr;

                break;
        }
    }

    protected function listTree(array $arr, array $aggs, array $order): array
    {
        $countmp = 0;

        for ($i = count($arr) - 1; $i >= 0; $i--) {
            if (isset($arr[$i - 1])) {
                $key_arr = array_keys($arr[$i]);

                if ($countmp === 0) {
                    if (!isset($arr[$i][$key_arr[0]]['date_histogram'])) {
                        $arr[$i][$key_arr[0]]['terms']['size'] = ($this->limit['from'] + 1) * $this->limit['size'];
                        if ($order) {
                            $arr[$i][$key_arr[0]]['terms']['order'] = $order['order'];
                        }
                    }

                    if (isset($aggs['aggs'])) {
                        if (isset($this->hasAggregrations['having']) && !empty($this->hasAggregrations['having'])) {
                            $aggs['aggs']['having'] = $this->hasAggregrations['having'];
                        }

                        $arr[$i][$key_arr[0]]['aggs'] = $aggs['aggs'];
                    }

                    $arr[$i][$key_arr[0]]['aggs']['top']['top_hits']['size'] = $this->topHits;
                    $countmp = 1;
                }

                $key_pre = array_keys($arr[$i - 1]);
                $arr[$i - 1][$key_pre[0]]['aggs'] = $arr[$i];

                unset($arr[$i]);
            } else {
                if (count($arr) === 1 && $countmp === 0) {
                    $key_arrs = array_keys($arr[$i]);

                    if (!isset($arr[$i][$key_arrs[0]]['date_histogram'])) {
                        $arr[$i][$key_arrs[0]]['terms']['size'] = ($this->limit['from'] + 1) * $this->limit['size'];
                        if ($order) {
                            $arr[$i][$key_arrs[0]]['terms']['order'] = $order['order'];
                        }
                    }

                    if (isset($aggs['aggs'])) {
                        if (isset($this->hasAggregrations['having']) && !empty($this->hasAggregrations['having'])) {
                            $aggs['aggs']['having'] = $this->hasAggregrations['having'];
                        }

                        $arr[$i][$key_arrs[0]]['aggs'] = $aggs['aggs'];
                    }

                    $arr[$i][$key_arrs[0]]['aggs']['top']['top_hits']['size'] = $this->topHits;
                    $countmp = 1;
                }
            }
        }

        return $arr;
    }

    protected function groupBy(array $arr)
    {
        $aggs = [];
        $agg = [];
        $agg_orderby = [];

        for ($i = 0; $i < count($arr); $i++) {
            if (strrpos($arr[$i]['base_expr'], ".")) {
                $term_tmp_arr = explode(".", $arr[$i]['base_expr']);

                if ($term_tmp_arr[1] !== 'keyword') {
                    $termk = $term_tmp_arr[1];
                    $termk_tmp = $termk;
                } else {
                    $termk = $arr[$i]['base_expr'];
                    $termk_tmp = $term_tmp_arr[0];
                }
            } else {
                $termk = $arr[$i]['base_expr'];
                $termk_tmp = $termk;
            }

            if (isset($this->firstGroup) && $this->firstGroup === '') {
                $this->firstGroup = $termk_tmp;
            }

            $agg[$i][$termk_tmp]['terms']['field'] = $termk;
            $agg[$i][$termk_tmp]['terms']['size'] = ($this->limit['from'] + 1) * $this->limit['size'];
        }

        if (isset($this->ast['SELECT']) && !empty($this->ast['SELECT'])) {
            foreach ($this->ast['SELECT'] as $v) {
                $this->topHits = 1;

                if ($v['expr_type'] === 'aggregate_function' || $v['expr_type'] === 'function') {
                    $lowerstr = strtolower($v['base_expr']);

                    switch ($lowerstr) {
                        case 'count':
                            if (strrpos($v['sub_tree'][0]['base_expr'], ".")) {
                                $term_tmp_arrs = explode(".", $v['sub_tree'][0]['base_expr']);
                                if ($term_tmp_arrs[1] !== 'keyword') {
                                    $cardinalitys[$v['alias']['name']]['cardinality']['field'] = $term_tmp_arrs[1];
                                } else {
                                    $cardinalitys[$v['alias']['name']]['cardinality']['field'] = $v['sub_tree'][0]['base_expr'];
                                }
                            } else {
                                $cardinalitys[$v['alias']['name']]['cardinality']['field'] = $v['sub_tree'][0]['base_expr'];
                            }

                            $agggs['aggs'] = $cardinalitys;
                            $aggs = array_merge_recursive($aggs, $agggs);

                            unset($cardinalitys);

                            break;
                        case 'sum':
                            if (strrpos($v['sub_tree'][0]['base_expr'], ".")) {
                                $term_tmp_arrs = explode(".", $v['sub_tree'][0]['base_expr']);

                                if (!isset($v['alias']['name'])) {
                                    $v['alias']['name'] = 'sum' . $term_tmp_arrs[1];
                                }

                                $cardinalitys[$v['alias']['name']]['sum']['field'] = $term_tmp_arrs[1];
                            } else {
                                if (!isset($v['alias']['name'])) {
                                    $v['alias']['name'] = 'sum' . $v['sub_tree'][0]['base_expr'];
                                }

                                $cardinalitys[$v['alias']['name']]['sum']['field'] = $v['sub_tree'][0]['base_expr'];
                            }

                            $agggs['aggs'] = $cardinalitys;
                            $aggs = array_merge_recursive($aggs, $agggs);

                            unset($cardinalitys);

                            break;
                        case 'min':
                            if (strrpos($v['sub_tree'][0]['base_expr'], ".")) {
                                $term_tmp_arrs = explode(".", $v['sub_tree'][0]['base_expr']);
                                $cardinalitys[$v['alias']['name']]['min']['field'] = $term_tmp_arrs[1];
                            } else {
                                $cardinalitys[$v['alias']['name']]['min']['field'] = $v['sub_tree'][0]['base_expr'];
                            }

                            $agggs['aggs'] = $cardinalitys;
                            $aggs = array_merge_recursive($aggs, $agggs);

                            unset($cardinalitys);

                            break;
                        case 'max':
                            if (strrpos($v['sub_tree'][0]['base_expr'], ".")) {
                                $term_tmp_arrs = explode(".", $v['sub_tree'][0]['base_expr']);
                                $cardinalitys[$v['alias']['name']]['max']['field'] = $term_tmp_arrs[1];
                            } else {
                                $cardinalitys[$v['alias']['name']]['max']['field'] = $v['sub_tree'][0]['base_expr'];
                            }

                            $agggs['aggs'] = $cardinalitys;
                            $aggs = array_merge_recursive($aggs, $agggs);

                            unset($cardinalitys);

                            break;
                        case 'avg':
                            if (strrpos($v['sub_tree'][0]['base_expr'], ".")) {
                                $term_tmp_arrs = explode(".", $v['sub_tree'][0]['base_expr']);
                                $cardinalitys[$v['alias']['name']]['avg']['field'] = $term_tmp_arrs[1];
                            } else {
                                $cardinalitys[$v['alias']['name']]['avg']['field'] = $v['sub_tree'][0]['base_expr'];
                            }

                            $agggs['aggs'] = $cardinalitys;
                            $aggs = array_merge_recursive($aggs, $agggs);

                            unset($cardinalitys);

                            break;
                        case 'concat_ws':
                            $tmp_script = '';
                            $tmp_ps = '';

                            if (isset($v['alias']) && !empty($v['alias'])) {
                                foreach ($agg as $kk => $ve) {
                                    $key_arr = array_keys($ve);

                                    if (isset($ve[$key_arr[0]]['terms']['field']) && $v['alias']['name'] === $ve[$key_arr[0]]['terms']['field']) {
                                        foreach ($v['sub_tree'] as $ke => $va) {
                                            if ($va['expr_type'] === 'const') {
                                                $tmp_ps = str_replace('"', '', $va['base_expr']);
                                                $tmp_ps = str_replace("'", "", $tmp_ps);
                                            }

                                            if ($va['expr_type'] === 'colref') {
                                                $tmp_script .= "'" . $tmp_ps . "' + doc['" . $va['base_expr'] . "'].value + ";
                                            }
                                        }

                                        $tmp_script = substr($tmp_script, 6, strlen($tmp_script) - 8);
                                        $agg[$kk][$key_arr[0]]['terms']['script']['source'] = $tmp_script;
                                        $agg[$kk][$key_arr[0]]['terms']['script']['lang'] = 'painless';

                                        unset($agg[$kk][$key_arr[0]]['terms']['field']);
                                    }
                                }
                            }
                            break;
                        case 'date_format':
                            $tmp_script = '';
                            $tmp_ps = '';

                            if (isset($v['alias']) && !empty($v['alias'])) {
                                foreach ($agg as $kk => $ve) {
                                    $key_arr = array_keys($ve);

                                    if (isset($ve[$key_arr[0]]['terms']['field']) && $v['alias']['name'] === $ve[$key_arr[0]]['terms']['field']) {
                                        for ($jj = 0; $jj <= count($v['sub_tree']) - 1; $jj++) {
                                            if ($v['sub_tree'][$jj]['expr_type'] === 'const') {
                                                $tmp_ps = str_replace('"', '', $v['sub_tree'][$jj]['base_expr']);
                                                $tmp_ps = str_replace("'", "", $tmp_ps);
                                                $tmp_ps = str_replace("%", "", $tmp_ps);
                                                $tmp_ps = str_replace("/", "", $tmp_ps);
                                                $tmp_ps = str_replace("-", "", $tmp_ps);

                                                switch ($tmp_ps) {
                                                    case 'Ymd':
                                                        $agg[$kk][$key_arr[0]]['date_histogram']['interval'] = "day";
                                                        break;
                                                    case 'Ym':
                                                        $agg[$kk][$key_arr[0]]['date_histogram']['interval'] = "month";
                                                        $agg[$kk][$key_arr[0]]['date_histogram']['format'] = "yyyy-MM";
                                                        break;
                                                    case 'Y':
                                                        $agg[$kk][$key_arr[0]]['date_histogram']['interval'] = "year";
                                                        $agg[$kk][$key_arr[0]]['date_histogram']['format'] = "yyyy";
                                                        break;
                                                    case 'Yu':
                                                        $agg[$kk][$key_arr[0]]['date_histogram']['interval'] = "week";
                                                        break;
                                                    case 'H':
                                                        $agg[$kk][$key_arr[0]]['date_histogram']['interval'] = "hour";
                                                        break;
                                                    case 'i':
                                                        $agg[$kk][$key_arr[0]]['date_histogram']['interval'] = "minute";
                                                        break;
                                                }
                                            }

                                            if ($v['sub_tree'][$jj]['expr_type'] === 'colref') {
                                                $agg[$kk][$key_arr[0]]['date_histogram']['field'] = $v['sub_tree'][$jj]['base_expr'];
                                                $agg[$kk][$key_arr[0]]['date_histogram']['format'] = "yyyy-MM-dd";
                                                $agg[$kk][$key_arr[0]]['date_histogram']['time_zone'] = "+08:00";
                                                unset($agg[$kk][$key_arr[0]]['terms']);
                                            }
                                        }
                                    }
                                }
                            }

                            break;
                    }

                    if (isset($this->ast['ORDER']) && !empty($this->ast['ORDER'])) {
                        foreach ($this->ast['ORDER'] as $vv) {
                            if ($vv['base_expr'] === $v['alias']['name']) {
                                $agg_orderby['order'][$vv['base_expr']] = $vv['direction'];
                            }
                        }
                    }
                }
            }
        }

        $tmp_tree = $this->listTree($agg, $aggs, $agg_orderby);

        $this->aggregations['aggs'] = $tmp_tree[0];
    }

    protected function orderBy(array $arr)
    {
        if (isset($this->ast['SELECT']) && !empty($this->ast['SELECT'])) {
            foreach ($this->ast['SELECT'] as $v) {
                foreach ($arr as $kk => $vv) {
                    if ($v['alias']) {
                        if ($v['alias']['name'] === $vv['base_expr']) {
                            unset($arr[$kk]);
                        }
                    }
                }
            }
        }

        foreach ($arr as &$va) {
            if (strrpos($va['base_expr'], ".")) {
                $term_tmp_arr = explode(".", $va['base_expr']);

                if ($term_tmp_arr[1] !== 'keyword') {
                    $termk = $term_tmp_arr[1];
                } else {
                    $termk = $va['base_expr'];
                }
            } else {
                $termk = $va['base_expr'];
            }

            $this->sort['sort'][][$termk]['order'] = $va['direction'];
        }
    }

    protected function limit(array $arr)
    {
        if (!isset($arr['offset'])) {
            $this->limit['from'] = 0;
        } else {
            $this->limit['from'] = (int) $arr['offset'];
        }

        if (!isset($arr['rowcount'])) {
            $this->limit['size'] = 10;
        } else {
            $this->limit['size'] = (int) $arr['rowcount'];
        }
    }

    protected function haveExt(array $arr, int $i)
    {
        if (!is_numeric($arr[$i]['base_expr'])) {
            $lowerstr = strtolower($arr[$i]['base_expr']);
        } else {
            $lowerstr = $arr[$i]['base_expr'];
        }

        switch ($lowerstr) {
            case '=':
                if ($arr[$i - 1]['base_expr'] === $arr[$i + 1]['base_expr']) {
                    break;
                }
                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                        $termk = $term_tmp_arr[1];
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_da_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_da_str = str_replace("'", "", $tmp_da_str);

                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_have !== '' && $this->tmp_lock_have !== $lowerstr) {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                        $this->count_tmp_filter_have++;
                    }

                    if (isset($this->hasAggregrations['having']['filter']['bool']['must'][0]) && $this->tmp_lock_str_have !== '' && $this->tmp_lock_str_have !== $lowerstr) {
                        if ($this->tmp_str_have === '') {
                            $this->count_tmp_have++;
                        } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                            $this->count_tmp_have++;
                        }
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_da_str;
                        $this->hasAggregrations['having']['filter']['bool']['must'][$this->count_tmp_have]['bool']['should'][] = $term;
                    } else {
                        $term['match_phrase'][$termk]['query'] = $tmp_da_str;
                        $this->hasAggregrations['having']['filter']['bool']['must'][$this->count_tmp_have]['bool']['should'][] = $term;
                    }

                    unset($term['match_phrase']);

                    $this->tmp_lock = $lowerstr;
                    $this->tmp_lock_str = $lowerstr;
                } else if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'and' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'and') {
                    if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                        $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                        $termk = $term_tmp_arr[1];
                    } else {
                        $termk = $arr[$i - 1]['base_expr'];
                    }

                    $tmp_da_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                    $tmp_da_str = str_replace("'", "", $tmp_da_str);

                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_str_have !== '' && $this->tmp_lock_str_have !== $lowerstr) {
                        if ($this->tmp_str_have === '') {
                            $this->count_tmp_have++;
                        } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                            $this->count_tmp_have++;
                        }
                    }

                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_have !== '' && $this->tmp_lock_have !== $lowerstr) {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                        $this->count_tmp_filter_have++;
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['match_phrase'][$termk . '.keyword']['query'] = $tmp_da_str;
                        $this->hasAggregrations['having']['filter']['bool']['must'][] = $term;
                    } else {
                        $term['match_phrase'][$termk]['query'] = $tmp_da_str;
                        $this->hasAggregrations['having']['filter']['bool']['must'][] = $term;
                    }

                    unset($term['match_phrase']);

                    $this->tmp_lock_str_have = $lowerstr;
                }

                $this->tmp_lock_have = $lowerstr;
                $this->tmp_str_have = $lowerstr;

                break;
            case 'in':
                if (strtolower($arr[$i - 1]['base_expr']) === 'not') {
                    break;
                }

                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    $termk = $term_tmp_arr[1];
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_have !== '' && $this->tmp_lock_have === $lowerstr) {
                    if ($this->tmp_str_filter_have === '') {
                        $this->count_tmp_filter_have++;
                    } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                        $this->count_tmp_filter_have++;
                    }
                } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                    $this->count_tmp_filter_have++;
                }

                if (isset($arr[$i + 1]['sub_tree']) && !empty($arr[$i + 1]['sub_tree'])) {
                    foreach ($arr[$i + 1]['sub_tree'] as &$vv) {
                        if (!is_numeric($vv['base_expr']) && $this->version === '5.x') {
                            $termk .= '.keyword';
                        }
                        $tmp_da_str = str_replace('"', '', $vv['base_expr']);
                        $tmp_da_str = str_replace("'", "", $tmp_da_str);
                        $this->hasAggregrations['having']['filter']['terms'][$termk][] = $tmp_da_str;
                    }
                }

                $this->tmp_lock_have = $lowerstr;
                $this->tmp_str_have = $termk;

                unset($termk);

                break;
            case 'not':
                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    $termk = $term_tmp_arr[1];
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_have !== '' && $this->tmp_lock_have === $lowerstr) {
                    if ($this->tmp_str_filter_have === '') {
                        $this->count_tmp_filter_have++;
                    } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                        $this->count_tmp_filter_have++;
                    }
                } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                    $this->count_tmp_filter_have++;
                }

                if (isset($arr[$i + 2]['sub_tree']) && !empty($arr[$i + 2]['sub_tree'])) {
                    foreach ($arr[$i + 2]['sub_tree'] as &$vv) {
                        if (!is_numeric($vv['base_expr']) && $this->version === '5.x') {
                            $termk .= '.keyword';
                        }
                        $tmp_da_str = str_replace('"', '', $vv['base_expr']);
                        $tmp_da_str = str_replace("'", "", $tmp_da_str);
                        $this->hasAggregrations['having']['filter']['bool']['must_not']['terms'][$termk][] = $tmp_da_str;
                    }
                }

                $this->tmp_lock_have = $lowerstr;
                $this->tmp_str_have = $termk;
                unset($termk);
                break;
            case '>':
                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    $termk = $term_tmp_arr[1];
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                $tmp_da_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_da_str = str_replace("'", "", $tmp_da_str);
                $is_date = strtotime($tmp_da_str) ? strtotime($tmp_da_str) : false;

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_have !== '' && $this->tmp_lock_have === $lowerstr) {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    }

                    if (isset($this->hasAggregrations['having']['filter']['bool']['must'][0]) && $this->tmp_lock_fi_have !== '' && $this->tmp_lock_fi_have === $lowerstr) {
                        if ($this->tmp_fi_have === '') {
                            $this->count_fi_have++;
                        } else if ($this->tmp_fi_have !== '' && $this->tmp_fi_have !== $termk) {
                            $this->count_fi_have++;
                        }
                    }

                    if (isset($this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][0]) && $this->tmp_lock_range_have !== '') {
                        if ($this->tmp_str_range_have === '') {
                            $this->count_tmp_range_have++;
                        } else if ($this->tmp_str_range_have !== '' && $this->tmp_str_range_have !== $termk) {
                            $this->count_tmp_range_have++;
                        }
                    }

                    $this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['gt'] = $tmp_da_str;

                    if (!isset($this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']) && $is_date) {
                        $this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone'] = "+08:00";
                    }
                } else {
                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_str_have !== '' && $this->tmp_lock_str_have !== $lowerstr) {
                        if ($this->tmp_str_have === '') {
                            $this->count_tmp_have++;
                        } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                            $this->count_tmp_have++;
                        }
                    }

                    if (!isset($this->hasAggregrations['having']['filter']['range']) && $this->tmp_lock_have !== '') {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                        $this->count_tmp_filter_have++;
                    }

                    $this->hasAggregrations['having']['filter']['range'][$termk]['gt'] = $tmp_da_str;

                    if (!isset($this->hasAggregrations['having']['filter']['range'][$termk]['time_zone']) && $is_date) {
                        $this->hasAggregrations['having']['filter']['range'][$termk]['time_zone'] = "+08:00";
                    }
                }

                $this->tmp_str_have = $termk;
                $this->tmp_lock_str_have = $lowerstr;
                $this->tmp_lock_have = $lowerstr;
                $this->tmp_lock_range_have = $lowerstr;
                $this->tmp_lock_fi_have = $lowerstr;

                break;
            case '>=':
                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    $termk = $term_tmp_arr[1];
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                $tmp_da_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_da_str = str_replace("'", "", $tmp_da_str);
                $is_date = strtotime($tmp_da_str) ? strtotime($tmp_da_str) : false;

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_have !== '' && $this->tmp_lock_have === $lowerstr) {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    }

                    if (isset($this->hasAggregrations['having']['filter']['bool']['must'][0]) && $this->tmp_lock_fi_have !== '' && $this->tmp_lock_fi_have === $lowerstr) {
                        if ($this->tmp_fi_have === '') {
                            $this->count_fi_have++;
                        } else if ($this->tmp_fi_have !== '' && $this->tmp_fi_have !== $termk) {
                            $this->count_fi_have++;
                        }
                    }

                    if (isset($this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][0]) && $this->tmp_lock_range_have !== '') {
                        if ($this->tmp_str_range_have === '') {
                            $this->count_tmp_range_have++;
                        } else if ($this->tmp_str_range_have !== '' && $this->tmp_str_range_have !== $termk) {
                            $this->count_tmp_range_have++;
                        }
                    }

                    $this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['gte'] = $tmp_da_str;

                    if (!isset($this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']) && $is_date) {
                        $this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone'] = "+08:00";
                    }
                } else {
                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_str_have !== '' && $this->tmp_lock_str_have !== $lowerstr) {
                        if ($this->tmp_str_have === '') {
                            $this->count_tmp_have++;
                        } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                            $this->count_tmp_have++;
                        }
                    }

                    if (!isset($this->hasAggregrations['having']['filter']['range']) && $this->tmp_lock_have !== '') {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                        $this->count_tmp_filter_have++;
                    }

                    $this->hasAggregrations['having']['filter']['range'][$termk]['gte'] = $tmp_da_str;

                    if (!isset($this->hasAggregrations['having']['filter']['range'][$termk]['time_zone']) && $is_date) {
                        $this->hasAggregrations['having']['filter']['range'][$termk]['time_zone'] = "+08:00";
                    }
                }

                $this->tmp_str_have = $termk;
                $this->tmp_lock_str_have = $lowerstr;
                $this->tmp_lock_have = $lowerstr;
                $this->tmp_lock_range_have = $lowerstr;
                $this->tmp_lock_fi_have = $lowerstr;

                break;
            case '<':
                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    $termk = $term_tmp_arr[1];
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                $tmp_da_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_da_str = str_replace("'", "", $tmp_da_str);
                $is_date = strtotime($tmp_da_str) ? strtotime($tmp_da_str) : false;

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_have !== '' && $this->tmp_lock_have === $lowerstr) {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    }

                    if (isset($this->hasAggregrations['having']['filter']['bool']['must'][0]) && $this->tmp_lock_fi_have !== '' && $this->tmp_lock_fi_have === $lowerstr) {
                        if ($this->tmp_fi_have === '') {
                            $this->count_fi_have++;
                        } else if ($this->tmp_fi_have !== '' && $this->tmp_fi_have !== $termk) {
                            $this->count_fi_have++;
                        }
                    }

                    if (isset($this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][0]) && $this->tmp_lock_range_have !== '') {
                        if ($this->tmp_str_range_have === '') {
                            $this->count_tmp_range_have++;
                        } else if ($this->tmp_str_range_have !== '' && $this->tmp_str_range_have !== $termk) {
                            $this->count_tmp_range_have++;
                        }
                    }

                    $this->hasAggregrations['having']['filter']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['lt'] = $tmp_da_str;

                    if (!isset($this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']) && $is_date) {
                        $this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone'] = "+08:00";
                    }
                } else {
                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_str_have !== '' && $this->tmp_lock_str_have === $lowerstr) {
                        if ($this->tmp_str_have === '') {
                            $this->count_tmp_have++;
                        }
                    }

                    if (!isset($this->hasAggregrations['having']['filter']['range']) && $this->tmp_lock_have !== '') {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                        $this->count_tmp_filter_have++;
                    }

                    $this->hasAggregrations['having']['filter']['range'][$termk]['lt'] = $tmp_da_str;

                    if (!isset($this->hasAggregrations['having']['filter']['range'][$termk]['time_zone']) && $is_date) {
                        $this->hasAggregrations['having']['filter']['range'][$termk]['time_zone'] = "+08:00";
                    }
                }

                $this->tmp_str_have = $termk;
                $this->tmp_lock_str_have = $lowerstr;
                $this->tmp_lock_have = $lowerstr;
                $this->tmp_lock_range_have = $lowerstr;
                $this->tmp_lock_fi_have = $lowerstr;

                break;
            case '<=':
                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    $termk = $term_tmp_arr[1];
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                $tmp_da_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_da_str = str_replace("'", "", $tmp_da_str);
                $is_date = strtotime($tmp_da_str) ? strtotime($tmp_da_str) : false;

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_have !== '' && $this->tmp_lock_have === $lowerstr) {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    }

                    if (isset($this->hasAggregrations['having']['filter']['bool']['must'][0]) && $this->tmp_lock_fi_have !== '' && $this->tmp_lock_fi_have === $lowerstr) {
                        if ($this->tmp_fi_have === '') {
                            $this->count_fi_have++;
                        } else if ($this->tmp_fi_have !== '' && $this->tmp_fi_have !== $termk) {
                            $this->count_fi_have++;
                        }
                    }

                    if (isset($this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][0]) && $this->tmp_lock_range_have !== '') {
                        if ($this->tmp_str_range_have === '') {
                            $this->count_tmp_range_have++;
                        } else if ($this->tmp_str_range_have !== '' && $this->tmp_str_range_have !== $termk) {
                            $this->count_tmp_range_have++;
                        }
                    }

                    $this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['lte'] = $tmp_da_str;

                    if (!isset($this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone']) && $is_date) {
                        $this->hasAggregrations['having']['bool']['must'][$this->count_fi_have]['bool']['should'][$this->count_tmp_range_have]['range'][$termk]['time_zone'] = "+08:00";
                    }
                } else {
                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_str_have !== '' && $this->tmp_lock_str_have === $lowerstr) {
                        if ($this->tmp_str_have === '') {
                            $this->count_tmp_have++;
                        } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                            $this->count_tmp_have++;
                        }
                    }

                    if (!isset($this->hasAggregrations['having']['filter']['range']) && $this->tmp_lock_have !== '') {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                        $this->count_tmp_filter_have++;
                    }

                    $this->hasAggregrations['having']['filter']['range'][$termk]['lte'] = $tmp_da_str;

                    if (!isset($this->hasAggregrations['having']['filter']['range'][$termk]['time_zone']) && $is_date) {
                        $this->hasAggregrations['having']['filter']['range'][$termk]['time_zone'] = "+08:00";
                    }
                }

                $this->tmp_str_have = $termk;
                $this->tmp_lock_str_have = $lowerstr;
                $this->tmp_lock_have = $lowerstr;
                $this->tmp_lock_range_have = $lowerstr;
                $this->tmp_lock_fi_have = $lowerstr;

                break;
            case 'like':
                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    $termk = $term_tmp_arr[1];
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                $tmp_la_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_la_str = str_replace("'", "", $tmp_la_str);

                if (isset($arr[$i + 2]['base_expr']) && strtolower($arr[$i + 2]['base_expr']) === 'or' || isset($arr[$i - 2]['base_expr']) && strtolower($arr[$i - 2]['base_expr']) === 'or') {
                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_have !== '' && $this->tmp_lock_have !== $lowerstr) {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    }

                    if (isset($this->hasAggregrations['having']['filter']['bool']['must'][0]) && $this->tmp_lock_fi_have !== '' && $this->tmp_lock_fi_have !== $lowerstr) {
                        if ($this->tmp_fi_have === '') {
                            $this->count_fi_have++;
                        } else if ($this->tmp_fi_have !== '' && $this->tmp_fi_have !== $termk) {
                            $this->count_fi_have++;
                        }
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['wildcard'][$termk . '.keyword'] = str_replace("%", "*", $tmp_la_str);
                        $this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][] = $term;
                    } else {
                        $term['wildcard'][$termk] = str_replace("%", "*", $tmp_la_str);
                        $this->hasAggregrations['having']['filter']['bool']['must'][$this->count_fi_have]['bool']['should'][] = $term;
                    }
                } else {
                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_str_have !== '' && $this->tmp_lock_str_have !== $lowerstr) {
                        if ($this->tmp_str_have === '') {
                            $this->count_tmp_have++;
                        } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                            $this->count_tmp_have++;
                        }
                    }

                    if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_have !== '' && $this->tmp_lock_have !== $lowerstr) {
                        if ($this->tmp_str_filter_have === '') {
                            $this->count_tmp_filter_have++;
                        } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                            $this->count_tmp_filter_have++;
                        }
                    }

                    if (!is_numeric($arr[$i + 1]['base_expr']) && $this->version === '8.x') {
                        $term['wildcard'][$termk . '.keyword'] = str_replace("%", "*", $tmp_la_str);
                        $this->hasAggregrations['having']['filter']['must'][$this->count_tmp_have]['bool']['must'][] = $term;
                    } else {
                        $term['wildcard'][$termk] = str_replace("%", "*", $tmp_la_str);
                        $this->hasAggregrations['having']['filter']['bool']['must'][] = $term;
                    }
                }

                unset($term['wildcard']);

                $this->tmp_lock_str_have = $lowerstr;
                $this->tmp_lock_have = $lowerstr;
                $this->tmp_lock_fi_have = $lowerstr;

                break;
            case 'between':
                if (strrpos($arr[$i - 1]['base_expr'], ".")) {
                    $term_tmp_arr = explode(".", $arr[$i - 1]['base_expr']);
                    $termk = $term_tmp_arr[1];
                } else {
                    $termk = $arr[$i - 1]['base_expr'];
                }

                if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_str_have !== '' && $this->tmp_lock_str_have !== $lowerstr) {
                    if ($this->tmp_str_have === '') {
                        $this->count_tmp_have++;
                    } else if ($this->tmp_str_have !== '' && $this->tmp_str_have !== $termk) {
                        $this->count_tmp_have++;
                    }
                }

                if (isset($this->hasAggregrations['having']['filter']) && $this->tmp_lock_have !== '' && $this->tmp_lock_have !== $lowerstr) {
                    if ($this->tmp_str_filter_have === '') {
                        $this->count_tmp_filter_have++;
                    } else if ($this->tmp_str_filter_have !== '' && $this->tmp_str_filter_have !== $termk) {
                        $this->count_tmp_filter_have++;
                    }
                }

                $tmp_da_str = str_replace('"', '', $arr[$i + 1]['base_expr']);
                $tmp_da_str = str_replace("'", "", $tmp_da_str);
                $is_date = strtotime($tmp_da_str) ? strtotime($tmp_da_str) : false;

                $this->hasAggregrations['having']['filter']['range'][$termk]['gte'] = $tmp_da_str;

                if (!isset($this->hasAggregrations['having']['filter']['range'][$termk]['time_zone']) && $is_date) {
                    $this->hasAggregrations['having']['filter']['range'][$termk]['time_zone'] = "+08:00";
                }

                $tmp_da_str = str_replace('"', '', $arr[$i + 3]['base_expr']);
                $tmp_da_str = str_replace("'", "", $tmp_da_str);

                $this->hasAggregrations['having']['filter']['range'][$termk]['lte'] = $tmp_da_str;
                $this->tmp_str_have = $termk;
                $this->tmp_lock_str_have = $lowerstr;
                $this->tmp_lock_have = $lowerstr;

                break;
        }
    }

    protected function having(array $arr)
    {
        if (isset($this->ast['HAVING']) && !empty($this->ast['HAVING'])) {
            for ($i = 0; $i < count($arr); $i++) {
                $this->haveExt($arr, $i);
            }

            $this->query['aggs']['having'] = $this->hasAggregrations['having'];
        }
    }

    protected function select(array $arr)
    {
        $tmp_source = [];

        foreach ($arr as $v) {
            if ($v['expr_type'] === 'aggregate_function') {
                if (strrpos($v['sub_tree'][0]['base_expr'], ".")) {
                    $term_tmp_arrs = explode(".", $v['sub_tree'][0]['base_expr']);
                    if ($term_tmp_arrs[1] === '*') {
                        continue;
                    }
                    if ($term_tmp_arrs[1] !== 'keyword') {
                        array_push($tmp_source, $term_tmp_arrs[1]);
                        if (isset($v['alias']['name'])) {
                            $this->query['aggs'][$v['alias']['name']]['stats']['field'] = $term_tmp_arrs[1];
                        } else {
                            $this->query['aggs'][$v['sub_tree'][0]['base_expr']]['stats']['field'] = $term_tmp_arrs[1];
                        }
                    } else {
                        array_push($tmp_source, $v['sub_tree'][0]['base_expr']);
                        if (isset($v['alias']['name'])) {
                            $this->query['aggs'][$v['alias']['name']]['cardinality']['field'] = $v['sub_tree'][0]['base_expr'];
                        } else {
                            $this->query['aggs'][$v['sub_tree'][0]['base_expr']]['cardinality']['field'] = $v['sub_tree'][0]['base_expr'];
                        }
                    }
                } else {
                    if ($v['sub_tree'][0]['base_expr'] === '*') {
                        continue;
                    }

                    array_push($tmp_source, $v['sub_tree'][0]['base_expr']);

                    if (isset($v['alias']['name'])) {
                        $this->query['aggs'][$v['alias']['name']]['stats']['field'] = $v['sub_tree'][0]['base_expr'];
                    } else {
                        $this->query['aggs'][$v['sub_tree'][0]['base_expr']]['stats']['field'] = $v['sub_tree'][0]['base_expr'];
                    }
                }
            } else {
                array_push($tmp_source, $v['base_expr']);
            }
        }

        if (!empty($tmp_source)) {
            $this->query['_source']['include'] = $tmp_source;
        }
    }
}
