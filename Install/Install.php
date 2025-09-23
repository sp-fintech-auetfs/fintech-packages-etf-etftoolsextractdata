<?php

namespace Apps\Fintech\Packages\Etf\Tools\Extractdata\Install;

use System\Base\BasePackage;
use System\Base\Providers\ModulesServiceProvider\TaskCallInstaller;

class Install extends BasePackage
{
    protected $taskCallInstaller;

    public function init()
    {
        $this->taskCallInstaller = new TaskCallInstaller;

        return $this;
    }

    public function install()
    {
        $this->preInstall();

        $this->installDb();

        $this->postInstall();

        return true;
    }

    protected function preInstall()
    {
        return true;
    }

    public function installDb()
    {
        return true;
    }

    public function postInstall()
    {
        $this->taskCallInstaller->installTaskCall($this);

        return true;
    }
}