<?php

namespace Tink\Queue\Beanstalk\Tests;

use Tink\Queue\Beanstalk;
use Tink\Queue\Beanstalk\Job;

class BeanstalkTest extends TestCase
{
    public function testQueue()
    {
        $this->assertInstanceOf(Beanstalk::class, $this->queue);
    }

    public function testConnect()
    {
        $connection = $this->queue->connection();
        $this->assertEquals(gettype($connection), 'resource');
    }

    public function testWatchTube()
    {
        $response = $this->queue->watch($this->testTube);
        $this->assertRegExp('/\d+/', $response);
    }

    public function testChooseTube()
    {
        $tube = $this->testTube;
        $response = $this->queue->choose($tube);

        $this->assertEquals($response, $tube);

        return $tube;
    }

    public function testListWatchTube()
    {
        $tube = $this->testTube;
        $this->queue->choose($tube);

        $this->assertEquals($tube, $this->queue->listTubeUsed());
    }

    public function testPut()
    {
        $data = [
            'name' => 'beanstalk',
            'date' => date('Y-m-d H:i:s'),
        ];

        $response = $this->queue->put($data);
        $this->assertRegExp('/\d+/', $response);

        return $response;
    }

    /**
     * @depends testPut
     */
    public function testJobPeek($jobId)
    {
        $job = $this->queue->jobPeek($jobId);
        $this->assertInstanceOf(Job::class, $job);
        $this->assertEquals($jobId, $job->getId());
        $this->assertArrayHasKey('name', $job->getBody());
    }

    public function testStats()
    {
        $stats = $this->queue->stats();
        $this->assertArrayHasKey('current-jobs-ready', $stats);
    }

    /**
     * @depends testChooseTube
     */
    public function testStatsTube($tube)
    {
        $stats = $this->queue->statsTube($tube);
        $this->assertEquals($tube, $stats['name']);
    }

    public function testDisConnect()
    {
        $this->queue->connection();
        $response = $this->queue->disconnect();
        $this->assertTrue($response);
    }
}
