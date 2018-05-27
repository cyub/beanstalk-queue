<?php

namespace Tink\Queue;

use Tink\Queue\Beanstalk\Job;
use Tink\Queue\Beanstalk\Exception;

class Beanstalk
{
    /**
     * Seconds to wait before putting the job in the ready queue
     * The job will be in the "delayed" state during this time
     */
    const DEFAULT_DELAY = 0;

    /**
     * Jobs with smaller priority values will be scheduled before jobs with larger priorities
     * The most urgent priority is 0, the least urgent priority is 4294967295
     */
    const DEFAULT_PRIORITY = 0;

    /**
     * Time to run - number of seconds to allow a worker to run this job
     * The minimum ttr is 1
     */
    const DEFAULT_TTR = 86400;

    /**
     * default tube name
     */
    const DEFAULT_TUBE = 'default';

    /**
     * default host name
     */
    const DEFAULT_HOST = '127.0.0.1';

    /**
     * default port
     */
    const DEFAULT_PORT = '13100';

    /**
     * the connection of beanstalk service
     * @var resource
     */
    protected $connection;

    /**
     * the parameter of connect beanstalk server
     * @var array
     */
    protected $parameters;

    public function __construct(array $parameters = [])
    {
        if (!isset($parameters['host'])) {
            $parameters['host'] = self::DEFAULT_HOST;
        }

        if (!isset($parameters['port'])) {
            $parameters['port'] = self::DEFAULT_PORT;
        }

        if (!isset($parameters['persistent'])) {
            $parameters['persistent'] = false;
        }

        $this->parameters = $parameters;
    }

    public function connection()
    {
        $connection = $this->connection;
        if (is_resource($connection)) {
            $this->disconnect();
        }

        $parameters = $this->parameters;
        try {
            if ($parameters['persistent']) {
                $connection = pfsockopen($parameters['host'], $parameters['port']);
            } else {
                $connection = fsockopen($parameters['host'], $parameters['port']);
            }
        } catch (\Exception $e) {
            throw new Exception($e->getMessage());
        }

        stream_set_timeout($connection, -1);

        $this->connection = $connection;

        return $connection;
    }

    /**
     * put job data into beanstalk
     * @param  mixed $data    the data of to beanstalk
     * @param  array  $options the options of put job data
     * @return int|boolean
     */
    public function put($data, array $options = [])
    {
        /**
         * priority
         * @var [type]
         */
        $priority = $options['priority'] ?? self::DEFAULT_PRIORITY;

        $delay = $options['delay'] ?? self::DEFAULT_DELAY;

        $ttr = $options['ttr'] ?? self::DEFAULT_TTR;

        // serialize data
        $serialize = serialize($data);

        $length = strlen($serialize);

        $this->write("put " . $priority . " " . $delay . " " . $ttr . " " . $length . "\r\n" . $serialize);
        $response = $this->readStatus();
        $status = $response[0];

        if ($status != 'INSERTED' && $status != 'BURIED') {
            return false;
        }

        return $response[1];
    }

    /**
     * reserve an job
     * @param  int|null $timeout reseve timeout
     * @return Object
     */
    public function reserve($timeout = null)
    {
        $command = 'reserve';
        if (!is_null($timeout)) {
            $command = 'reserve-with-timeout ' . intval($timeout);
        }

        $this->write($command);

        $response = $this->readStatus();
        if ($response[0] != 'RESERVED') {
            return false;
        }

        return new Job($this, $response[1], unserialize($this->read($response[1])));
    }

    /**
     * choose an tube
     * @param  string $tube the name of tube
     * @return string|boolean
     */
    public function choose($tube)
    {
        $this->write('use ' . $tube);

        $response = $this->readStatus();

        if ($response[0] != 'USING') {
            return false;
        }

        return $response[1];
    }

    /**
     * watch tube
     * @param  string $tube the tube need to watch
     * @return string|boolean
     */
    public function watch($tube)
    {
        $this->write('watch ' . $tube);

        $response = $this->readStatus();
        if ($response[0] != 'WATCHING') {
            return false;
        }

        return $response[1];
    }

    /**
     * ingore tube to watch
     * @param  string $tube
     * @return string|boolean
     */
    public function ignore($tube)
    {
        $this->write('ignore '. $tube);

        $response = $this->readStatus();
        if ($response[0] != 'WATCHING') {
            return false;
        }

        return $response[1];
    }

    /**
     * pause tube in delay seconds
     * @param  string $tube
     * @param  int $delay delay second to pause
     * @return boolean
     */
    public function pauseTube($tube, $delay)
    {
        $this->write('pause-tube ' . $tube . ' ' . $delay);

        $response = $this->readStatus();
        if ($response[0] != 'PAUSED') {
            return false;
        }

        return true;
    }

    public function kick($bound)
    {
        $this->write('kick ' . $bound);

        $response = $this->readStatus();
        if ($response[0] != 'KICKED') {
            return false;
        }

        return intval($response[1]);
    }

    /**
     * the stats of beankstalk server
     * @return array|boolean
     */
    public function stats()
    {
        $this->write('stats');

        $response = $this->readYaml();
        if ($response[0] != 'OK') {
            return false;
        }

        return $response[2];
    }

    /**
     * the stats of special tube
     * @param  $tube the tube to stats
     * @return array|boolean
     */
    public function statsTube($tube)
    {
        $this->write('stats-tube ' . $tube);

        $response = $this->readYaml();
        if ($response[0] != 'OK') {
            return false;
        }

        return $response[2];
    }

    /**
     * list the used tube
     * @return string|boolean
     */
    public function listTubeUsed()
    {
        $this->write('list-tube-used');

        $response = $this->readStatus();
        if ($response[0] != 'USING') {
            return false;
        }

        return $response[1];
    }

    public function peekReady()
    {
        $this->write('peek-ready');

        $response = $this->readStatus();

        if ($response[0] != 'FOUND') {
            return false;
        }

        return new Job($this, $response[1], unserialize($this->read($response[2])));
    }

    public function peekBuried()
    {
        $this->write('peek-buried');

        $response = $this->readStatus();

        if ($response[0] != 'FOUND') {
            return false;
        }

        return new Job($this, $response[1], unserialize($this->read($response[2])));
    }

    public function peekDelayed()
    {
        if (!$this->write('peek-delayed')) {
            return false;
        }

        $response = $this->readStatus();
        if ($response[0] != 'FOUND') {
            return false;
        }

        return new Job($this, $response[1], unserialize($this->read($response[2])));
    }

    public function jobPeek($id)
    {
        $this->write('peek ' . $id);

        $response = $this->readStatus();
        if ($response[0] != 'FOUND') {
            return false;
        }

        return new Job($this, $response[1], unserialize($this->read($response[2])));
    }

    /**
     * the status of exec beanstalk command
     * @return array
     */
    final public function readStatus()
    {
        $status = $this->read();
        if ($status === false) {
            return [];
        }

        return explode(' ', $status);
    }

    final public function readYaml()
    {
        $response = $this->readStatus();
        $status = $response[0];

        if (count($response) > 1) {
            $numberOfBytes = $response[1];
            $response = $this->read();
            $data = yaml_parse($response);
        } else {
            $numberOfBytes = 0;
            $data = [];
        }

        return [
            $status,
            $numberOfBytes,
            $data
        ];
    }

    /**
     * read content from beanstalk
     * @param  integer $length the length to read
     * @return mixed
     */
    public function read($length = 0)
    {
        $connection = $this->connection;

        if (!is_resource($connection)) {
            $connection = $this->connection();
            if (!$connection) {
                return false;
            }
        }

        if ($length) {
            if (feof($connection)) {
                return false;
            }

            $data = rtrim(stream_get_line($connection, $length + 2), "\r\n");
            $streamMetaData = stream_get_meta_data($connection);

            if (isset($streamMetaData['timeout'])) {
                throw new Exception('Connection time out');
            }
        } else {
            $data = stream_get_line($connection, 16384, "\r\n");
        }

        if ($data == 'UNKNOWN_COMMAND') {
            throw new  Exception('UNKNOWN_COMMAND');
        }

        if ($data == 'JOB_TOO_BIG') {
            throw new Exception('JOB_TOO_BIG');
        }

        if ($data == 'BAD_FORMAT') {
            throw new Exception('BAD_FORMAT');
        }

        if ($data == 'OUT_OF_MEMORY') {
            throw new Exception('OUT_OF_MEMORY');
        }

        return $data;
    }

    /**
     * write content to beanstalk
     * @param  mixed $data
     * @return boolean
     */
    public function write($data)
    {
        $connection = $this->connection;

        if (!is_resource($connection)) {
            $connection = $this->connection();
            if (!$connection) {
                return false;
            }
        }

        $packet = $data . "\r\n";
        return fwrite($connection, $packet, strlen($packet));
    }

    /**
     * disconnect beanstalk server
     * @return boolean
     */
    public function disconnect()
    {
        $connection = $this->connection;
        if (!is_resource($connection)) {
            return false;
        }

        fclose($connection);
        $this->connection = null;

        return true;
    }

    /**
     * quit beanstalk serve
     * @return boolean
     */
    public function quit()
    {
        $this->write('quit');
        $this->disconnect();

        return gettype($this->connection) == 'resource';
    }
}
