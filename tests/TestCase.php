<?php

namespace Tink\Queue\Beanstalk\Tests;

use PHPUnit\Framework\TestCase as BaseTestCase;
use Tink\Queue\Beanstalk;

class TestCase extends BaseTestCase
{
    public function setUp()
    {
        parent::setUp();

        $parameters = [
            'host' => getenv('BEANSTALK_HOST'),
            'port' => getenv('BEANSTALK_PORT'),
            'persistent' => getenv('BEANSTALK_PERSISTENT'),
        ];

        $this->testTube = getenv('BEANSTALK_TUBE');
        $this->queue = new Beanstalk($parameters);
    }
}
