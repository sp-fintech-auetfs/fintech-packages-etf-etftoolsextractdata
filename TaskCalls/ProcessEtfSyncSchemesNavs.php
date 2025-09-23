<?php

namespace Apps\Fintech\Packages\Etf\Tools\Extractdata\TaskCalls;

use Apps\Fintech\Packages\Etf\Tools\Extractdata\EtfToolsExtractdata;
use System\Base\Providers\BasepackagesServiceProvider\Packages\Workers\Calls;

class ProcessEtfSyncSchemesNavs extends Calls
{
    protected $funcDisplayName = 'Sync ETF Schemes & Navs';

    protected $funcDescription = 'Sync ETF Schemes & Navs Github.';

    protected $args;

    public function run(array $args = [])
    {
        $thisCall = $this;

        return function() use ($thisCall, $args) {
            $thisCall->updateJobTask(2, $args);

            $this->args = $this->extractCallArgs($thisCall, $args);

            if (!$this->args) {
                return;
            }

            try {
                $etfExtractDataPackage = new EtfToolsExtractdata;

                if (isset($this->args['downloadnav']) &&
                    $this->args['downloadnav'] == 'true'
                ) {
                    $etfExtractDataPackage->downloadEtfData();
                    $etfExtractDataPackage->extractEtfData();
                    $etfExtractDataPackage->processEtfData();
                }
            } catch (\throwable $e) {
                if ($this->config->logs->exceptions) {
                    $this->logger->logExceptions->critical(json_trace($e));
                }

                $thisCall->packagesData->responseMessage = 'Exception: Please check exceptions log for more details.';

                $thisCall->packagesData->responseCode = 1;

                if (isset($etfExtractDataPackage->responseData)) {
                    $thisCall->packagesData->responseData = $etfExtractDataPackage->responseData;
                }

                $this->addJobResult($thisCall->packagesData, $args);

                $thisCall->updateJobTask(3, $args);

                return;
            }

            $thisCall->packagesData->responseMessage = $etfExtractDataPackage->packagesData->responseMessage ?? 'Ok';

            $thisCall->packagesData->responseCode = $etfExtractDataPackage->packagesData->responseCode ?? 0;

            $this->addJobResult($etfExtractDataPackage->packagesData->responseData ?? [], $args);

            $thisCall->updateJobTask(3, $args);
        };
    }
}