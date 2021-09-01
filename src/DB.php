<?php

namespace mgboot\databasex;

use Illuminate\Support\Collection;
use mgboot\AppConf;
use mgboot\Cast;
use mgboot\constant\Regexp;
use mgboot\poolx\ConnectionInterface;
use mgboot\poolx\PdoConnection;
use mgboot\poolx\PoolContext;
use mgboot\swoole\Swoole;
use mgboot\util\ExceptionUtils;
use mgboot\util\FileUtils;
use mgboot\util\JsonUtils;
use mgboot\util\StringUtils;
use PDO;
use PDOStatement;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Throwable;

final class DB
{
    /**
     * @var LoggerInterface|null
     */
    private static $logger = null;

    /**
     * @var bool
     */
    private static $debugLogEnabled = false;

    /**
     * @var array
     */
    private static $connectionSettings = [];

    /**
     * @var bool
     */
    private static $poolEnabled = false;

    /**
     * @var string
     */
    private static $cacheDir = 'classpath:cache';

    /**
     * @var array
     */
    private static $tableSchemas = [];

    private function __construct()
    {
    }

    private function __clone()
    {
    }

    public static function withLogger(LoggerInterface $logger): void
    {
        self::$logger = $logger;
    }

    public static function enableDebugLog(): void
    {
        self::$debugLogEnabled = true;
    }

    public static function withConnectionSettings(array $settings): void{
        self::$connectionSettings = $settings;
    }

    public static function enablePool(): void
    {
        self::$poolEnabled = true;
    }

    public static function withCacheDir(string $dir): void
    {
        if ($dir !== '' && is_dir($dir) && is_writable($dir)) {
            self::$cacheDir = $dir;
        }
    }

    public static function buildTableSchemas(): void
    {
        $inDevMode = AppConf::getEnv() === 'dev';

        if ($inDevMode) {
            return;
        }

        self::$tableSchemas = self::buildTableSchemasFromCacheFile();
    }

    public static function getTableSchema(string $tableName): array
    {
        $tableName = str_replace('`', '', $tableName);

        if (strpos($tableName, '.') !== false) {
            $tableName = StringUtils::substringAfterLast($tableName, '.');
        }

        if (AppConf::getEnv() === 'dev') {
            $schemas = self::buildTableSchemasInternal();
        } else {
            $schemas = self::$tableSchemas;

            if (empty($schemas)) {
                self::buildTableSchemas();
                $schemas = self::$tableSchemas;
            }
        }

        return is_array($schemas) && isset($schemas[$tableName]) ? $schemas[$tableName] : [];
    }

    public static function table(string $tableName): QueryBuilder
    {
        return QueryBuilder::create($tableName);
    }

    public static function raw(string $expr): Expression
    {
        return Expression::create($expr);
    }

    public static function selectBySql(string $sql, array $params = []): Collection
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return collect([]);
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            return collect($stmt->fetchAll());
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            self::freeConnection($conn);
        }
    }

    public static function firstBySql(string $sql, array $params = []): ?array
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return null;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            $data = $stmt->fetch();
            return is_array($data) ? $data : null;
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            self::freeConnection($conn);
        }
    }

    public static function countBySql(string $sql, array $params = []): int
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            return (int) $stmt->fetchColumn();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            self::freeConnection($conn);
        }
    }

    public static function insertBySql(string $sql, array $params = []): int
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            return (int) $pdo->lastInsertId();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            self::freeConnection($conn);
        }
    }

    public static function updateBySql(string $sql, array $params = []): int
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            return $stmt->rowCount();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            self::freeConnection($conn);
        }
    }

    /**
     * @param string $sql
     * @param array $params
     * @return int|float|string
     */
    public static function sumBySql(string $sql, array $params = [])
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            $value = $stmt->fetchColumn();

            if (is_int($value) || is_float($value)) {
                return $value;
            }

            if (!is_string($value) || $value === '') {
                return 0;
            }

            if (StringUtils::isInt($value)) {
                return Cast::toInt($value);
            }

            if (StringUtils::isFloat($value)) {
                return bcadd($value, 0, 2);
            }

            return 0;
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            self::freeConnection($conn);
        }
    }

    public static function deleteBySql(string $sql, array $params = []): int
    {
        return self::updateBySql($sql, $params);
    }

    public static function executeSql(string $sql, array $params = []): void
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            self::freeConnection($conn);
        }
    }

    public static function transations(callable $callback): void
    {
        try {
            if (Swoole::inCoroutineMode(true)) {
                $conn = PoolContext::getConnection(PoolContext::POOL_TYPE_DB, 2.0);
            } else {
                $conn = PdoConnection::create(self::$connectionSettings);
            }

            if (!($conn instanceof PdoConnection)) {
                throw new RuntimeException('fail to get database connection');
            }
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        /* @var PdoConnection $conn */
        $conn->inTranstionMode(true);
        TxManager::addConnection($conn);

        try {
            $conn->getRealConnection()->beginTransaction();
            $callback();
            $conn->getRealConnection()->commit();
            self::freeConnection($conn);
        } catch (Throwable $ex) {
            $conn->getRealConnection()->rollBack();
            $ex = self::wrapAsDbException($ex);
            self::freeConnection($conn, $ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            $conn->inTranstionMode(false);
            TxManager::removeConnection();
        }
    }

    private static function getConnection(): array
    {
        $conn = TxManager::getConnection();

        if ($conn instanceof ConnectionInterface) {
            return [$conn, $conn->getRealConnection()];
        }

        if (self::$poolEnabled && Swoole::inCoroutineMode(true)) {
            $conn = PoolContext::getConnection(PoolContext::POOL_TYPE_DB, 2.0);
        } else {
            $conn = PdoConnection::create(self::$connectionSettings);
        }

        return [$conn, $conn->getRealConnection()];
    }

    /**
     * @param mixed $conn
     * @param Throwable|null $ex
     */
    private static function freeConnection($conn, ?Throwable $ex = null): void
    {
        if ($conn instanceof ConnectionInterface && !$conn->inTranstionMode()) {
            $conn->free($ex);
        }
    }

    private static function pdoBindParams(PDOStatement $stmt, array $params): void
    {
        if (empty($params)) {
            return;
        }

        foreach ($params as $i => $value) {
            if ($value === null) {
                $stmt->bindValue($i + 1, null, PDO::PARAM_NULL);
                continue;
            }

            if (is_int($value)) {
                $stmt->bindValue($i + 1, $value, PDO::PARAM_INT);
                continue;
            }

            if (is_float($value)) {
                $stmt->bindValue($i + 1, "$value");
                continue;
            }

            if (is_string($value)) {
                $stmt->bindValue($i + 1, $value);
                continue;
            }

            if (is_bool($value)) {
                $stmt->bindValue($i + 1, $value, PDO::PARAM_BOOL);
                continue;
            }

            if (is_array($value)) {
                throw new DbException(null, 'fail to bind param, param type: array');
            }

            if (is_resource($value)) {
                throw new DbException(null, 'fail to bind param, param type: resource');
            }

            if (is_object($value)) {
                throw new DbException(null, 'fail to bind param, param type: ' . get_class($value));
            }
        }
    }

    private static function buildTableSchemasInternal(): array
    {
        try {
            $conn = PdoConnection::create(self::$connectionSettings);
        } catch (Throwable $ex) {
            return [];
        }

        $tables = [];

        try {
            $stmt = $conn->getRealConnection()->prepare('SHOW TABLES');
            $stmt->execute();
            $records = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (!is_array($records) || empty($records)) {
                $conn->close();
                return [];
            }

            foreach ($records as $record) {
                foreach ($record as $key => $value) {
                    if (strpos($key, 'Tables_in') !== false) {
                        $tables[] = trim($value);
                        break;
                    }
                }
            }
        } catch (Throwable $ex) {
            $conn->close();
            return [];
        }

        if (empty($tables)) {
            $conn->close();
            return [];
        }

        $schemas = [];

        foreach ($tables as $tableName) {
            try {
                $stmt = $conn->getRealConnection()->prepare("DESC $tableName");
                $stmt->execute();
                $items = $stmt->fetchAll(PDO::FETCH_ASSOC);

                if (!is_array($items) || empty($items)) {
                    continue;
                }

                $schema = array_map(function ($item) {
                    $fieldName = $item['Field'];
                    $nullable = stripos($item['Null'], 'YES') !== false;
                    $isPrimaryKey = $item['Key'] === 'PRI';
                    $defaultValue = $item['Default'];
                    $autoIncrement = $item['Extra'] === 'auto_increment';
                    $parts = preg_split(Regexp::SPACE_SEP, $item['Type']);

                    if (strpos($parts[0], '(') !== false) {
                        $fieldType = StringUtils::substringBefore($parts[0], '(');
                        $fieldSize = str_replace($fieldType, '', $parts[0]);
                    } else {
                        $fieldType = $parts[0];
                        $fieldSize = '';
                    }

                    if (!StringUtils::startsWith($fieldSize, '(') || !StringUtils::endsWith($fieldSize, ')')) {
                        $fieldSize = '';
                    } else {
                        $fieldSize = rtrim(ltrim($fieldSize, '('), ')');
                    }

                    if (is_numeric($fieldSize)) {
                        $fieldSize = (int) $fieldSize;
                    }

                    $unsigned = stripos($item['Type'], 'unsigned') !== false;

                    return compact(
                        'fieldName',
                        'fieldType',
                        'fieldSize',
                        'unsigned',
                        'nullable',
                        'defaultValue',
                        'autoIncrement',
                        'isPrimaryKey'
                    );
                }, $items);
            } catch (Throwable $ex) {
                $schema = null;
            }

            if (!is_array($schema) || empty($schema)) {
                continue;
            }

            $schemas[$tableName] = $schema;
        }

        $conn->close();
        return $schemas;
    }

    private static function buildTableSchemasFromCacheFile(): array
    {
        $dir = FileUtils::getRealpath(self::$cacheDir);

        if (!is_dir($dir)) {
            return self::buildTableSchemasInternal();
        }

        $cacheFile = "$dir/table_schemas.php";
        $schemas = [];

        if (is_file($cacheFile)) {
            try {
                $schemas = include($cacheFile);
            } catch (Throwable $ex) {
                $schemas = [];
            }
        }

        if (is_array($schemas) && !empty($schemas)) {
            return $schemas;
        }

        $schemas = self::buildTableSchemasInternal();
        self::writeTableSchemasToCacheFile($schemas);
        return $schemas;
    }

    private static function writeTableSchemasToCacheFile(array $schemas): void
    {
        if (empty($schemas)) {
            return;
        }

        $dir = FileUtils::getRealpath(self::$cacheDir);

        if (!is_dir($dir) || !is_writable($dir)) {
            return;
        }

        $cacheFile = "$dir/table_schemas.php";
        $fp = fopen($cacheFile, 'w');

        if (!is_resource($fp)) {
            return;
        }

        $sb = [
            "<?php\n",
            'return ' . var_export($schemas, true) . ";\n"
        ];

        flock($fp, LOCK_EX);
        fwrite($fp, implode('', $sb));
        flock($fp, LOCK_UN);
        fclose($fp);
    }

    private static function wrapAsDbException(Throwable $ex): DbException
    {
        if ($ex instanceof DbException) {
            return $ex;
        }

        return new DbException(null, $ex->getMessage());
    }

    private static function logSql(string $sql, ?array $params = null): void
    {
        $logger = self::$logger;

        if (!($logger instanceof LoggerInterface) || !self::$debugLogEnabled) {
            return;
        }

        $logger->info($sql);

        if (is_array($params) && !empty($params)) {
            $logger->debug('params: ' . JsonUtils::toJson($params));
        }
    }

    /**
     * @param string|Throwable $msg
     */
    private static function writeErrorLog($msg): void
    {
        $logger = self::$logger;

        if (!($logger instanceof LoggerInterface)) {
            return;
        }

        if ($msg instanceof Throwable) {
            $msg = ExceptionUtils::getStackTrace($msg);
        }

        $logger->error($msg);
    }
}
