<?php

namespace mgboot\databasex;

final class Expression
{
    /**
     * @var string
     */
    private $expr;

    private function __construct(string $expr)
    {
        $this->expr = $expr;
    }

    private function __clone()
    {
    }

    public static function create(string $expr): Expression
    {
        return new self($expr);
    }

    public function getExpr(): string
    {
        return $this->expr;
    }
}
