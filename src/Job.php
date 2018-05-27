<?php

namespace Tink\Queue\Beanstalk;

use Tink\Queue\Beanstalk;

class Job
{
    protected $id;

    protected $body;

    protected $queue;

    public function __construct(Beanstalk $queue, $id, $body)
    {
        $this->queue = $queue;
        $this->id = $id;
        $this->body = $body;
    }

    public function getId()
    {
        return $this->id;
    }

    public function getBody()
    {
        return $this->body;
    }

    public function delete()
    {
        $queue = $this->queue;
        $queue->write('delete ' . $this->id);

        $response = $queue->readStatus();
        return $response[0] == 'DELETED';
    }

    public function release($priority = 100, $delay = 0)
    {
        $queue = $this->queue;
        $queue->write('release ' . $this->id . ' ' . $priority . ' ' . $delay);

        $response = $queue->readStatus();
        return $response[0] == 'RELEASED';
    }

    public function bury($priority = 100)
    {
        $queue = $this->queue;
        $queue->write('bury ' . $this->id . ' ' . $priority);

        $response = $queue->readStatus();
        return $response[0] == 'BURIED';
    }

    public function touch()
    {
        $queue = $this->queue;
        $queue->write('touch ' . $this->id);

        $response = $queue->readStatus();
        return $response[0] == 'TOUCHED';
    }

    public function kick()
    {
        $queue = $this->queue;
        $queue->write('kick-job ' .$this->id);

        $response = $queue->readStatus();
        return $response[0] == 'KICKED';
    }

    public function stats()
    {
        $queue = $this->queue;
        $queue->write('stats-job ' . $this->id);

        $response = $queue->readYaml();
        if ($response[0] == 'NOT_FOUND') {
            return false;
        }

        return $response[2];
    }

    public function __wakeup()
    {
        if (!is_string($this->id)) {
            throw new Exception("Unexpected inconsistency in Tink\\Queue\\Beanstalk\\Job::__wakeup() - possible break-in attempt!");
        }
    }
}
