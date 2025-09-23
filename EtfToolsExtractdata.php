<?php

namespace Apps\Fintech\Packages\Etf\Tools\Extractdata;

use Apps\Fintech\Packages\Etf\Amcs\EtfAmcs;
use Apps\Fintech\Packages\Etf\Categories\EtfCategories;
use Apps\Fintech\Packages\Etf\Extractdata\Settings;
use Apps\Fintech\Packages\Etf\Portfolios\EtfPortfolios;
use Apps\Fintech\Packages\Etf\Schemes\EtfSchemes;
use Apps\Fintech\Packages\Etf\Schemes\Model\AppsFintechEtfSchemesSnapshotsNavsChunks;
use Apps\Fintech\Packages\Etf\Schemes\Model\AppsFintechEtfSchemesSnapshotsNavsRollingReturns;
use Apps\Fintech\Packages\Etf\Types\EtfTypes;
use League\Csv\Reader;
use League\Csv\Statement;
use League\Flysystem\FilesystemException;
use League\Flysystem\UnableToCheckExistence;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToWriteFile;
use Phalcon\Db\Enum;
use System\Base\BasePackage;
use System\Base\Providers\DatabaseServiceProvider\Sqlite;

class EtfToolsExtractdata extends BasePackage
{
    protected $now;

    protected $today;

    protected $year;

    protected $previousDay;

    protected $weekAgo;

    protected $sourceLink;

    protected $destDir = 'apps/Fintech/Packages/Etf/Tools/Extractdata/Data/';

    protected $destFile;

    protected $trackCounter = 0;

    public $method;

    protected $apiClient;

    protected $apiClientConfig;

    protected $settings = Settings::class;

    protected $navsPackage;

    protected $categoriesPackage;

    protected $amcsPackage;

    protected $schemesPackage;

    protected $schemes = [];

    protected $amcs = [];

    protected $categories = [];

    protected $parsedCarbon = [];

    protected $marketIndexEtfs = [];

    protected $betasharesEtfs = [];

    protected $combinedEtfs = [];

    public function onConstruct()
    {
        if (!is_dir(base_path($this->destDir))) {
            if (!mkdir(base_path($this->destDir), 0777, true)) {
                return false;
            }
        }

        //Increase Exectimeout to 5 hours as this process takes time to extract and merge data.
        if ((int) ini_get('max_execution_time') < 18000) {
            set_time_limit(18000);
        }

        //Increase memory_limit to 1G as the process takes a bit of memory to process the array.
        if ((int) ini_get('memory_limit') < 1024) {
            ini_set('memory_limit', '1024M');
        }

        $this->now = \Carbon\Carbon::now(new \DateTimeZone('Australia/Melbourne'));
        $this->year = $this->now->year;
        $this->today = $this->now->toDateString();
        $this->previousDay = $this->now->subDay(1)->toDateString();
        $this->now = $this->now->addDay(1);
        $this->weekAgo = $this->now->subDay(7)->toDateString();
        $this->now = $this->now->addDay(7);

        parent::onConstruct();
    }

    public function __call($method, $arguments)
    {
        if (method_exists($this, $method)) {
            if (PHP_SAPI !== 'cli') {
                $this->basepackages->progress->updateProgress($method, null, false);

                $call = call_user_func_array([$this, $method], $arguments);

                $callResult = $call;

                if ($call !== false) {
                    $call = true;
                }

                $this->basepackages->progress->updateProgress($method, $call, false);

                return $callResult;
            }

            call_user_func_array([$this, $method], $arguments);
        }
    }

    protected function alphabet_to_number($string)
    {
        $string = strtoupper($string);
        $length = strlen($string);
        $number = 0;
        $level = 0;

        $number = '';

        while ($length > $level ) {
            $char = $string[$level];

            $c = ord($char) - 64;

            if ($c < 0) {//char is number
                $number .= $char;
            } else {
                $number .= $c;
            }

            $level++;
        }

        return (int) $number;
    }

    protected function downloadEtfNavsData()
    {
        $this->method = 'downloadEtfNavsData';

        $this->sourceLink = 'https://github.com/sp-fintech-auetfs/historical-etf-data/raw/refs/heads/main/data/all.zip';

        $this->destFile = base_path($this->destDir) . $this->today . '.zip';

        try {
            //File is already downloaded
            if ($this->localContent->fileExists($this->destDir . $this->today . '.zip')) {
                $remoteSize = (int) getRemoteFilesize($this->sourceLink);

                $localSize = $this->localContent->fileSize($this->destDir . $this->today . '.zip');

                if ($remoteSize === $localSize) {
                    return true;
                }
            }
        } catch (FilesystemException | UnableToCheckExistence | UnableToRetrieveMetadata | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        return $this->downloadData($this->sourceLink, $this->destFile);
    }

    protected function downloadData($url, $sink)
    {
        $download = $this->remoteWebContent->request(
            'GET',
            $url,
            [
                'progress' => function(
                    $downloadTotal,
                    $downloadedBytes,
                    $uploadTotal,
                    $uploadedBytes
                ) {
                    $counters =
                            [
                                'downloadTotal'     => $downloadTotal,
                                'downloadedBytes'   => $downloadedBytes,
                                'uploadTotal'       => $uploadTotal,
                                'uploadedBytes'     => $uploadedBytes
                            ];

                    if ($downloadedBytes === 0) {
                        return;
                    }

                    //Trackcounter is needed as guzzelhttp runs this in a while loop causing too many updates with same download count.
                    //So this way, we only update progress when there is actually an update.
                    if ($downloadedBytes === $this->trackCounter) {
                        return;
                    }

                    $this->trackCounter = $downloadedBytes;

                    if (PHP_SAPI !== 'cli') {
                        if ($downloadedBytes === $downloadTotal) {
                            $this->basepackages->progress->updateProgress($this->method, true, false, null, $counters);
                        } else {
                            $this->basepackages->progress->updateProgress($this->method, null, false, null, $counters);
                        }
                    }
                },
                'verify'            => false,
                'connect_timeout'   => 5,
                'sink'              => $sink
            ]
        );

        $this->trackCounter = 0;

        if ($download->getStatusCode() === 200) {
            return true;
        }

        $this->addResponse('Download resulted in : ' . $download->getStatusCode(), 1);

        return false;
    }

    protected function cleanup($type)
    {
        try {
            $scanDir = $this->basepackages->utils->scanDir($this->destDir, false);

            if ($scanDir && count($scanDir['files']) > 0) {
                foreach ($scanDir['files'] as $file) {
                    if ($type === '-funds') {
                        if (!str_starts_with($file, $this->year) &&
                            str_contains($file, $type)
                        ) {
                            $this->localContent->delete($file);
                        }
                    } else {
                        if (!str_starts_with($file, $this->today) &&
                            str_contains($file, $type)
                        ) {
                            $this->localContent->delete($file);
                        }
                    }
                }
            }
        } catch (UnableToDeleteFile | \throwable | FilesystemException $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        return true;
    }

    protected function extractEtfNavsData()
    {
        $this->method = 'extractEtfNavsData';

        try {
            if (!$this->localContent->fileExists($this->destDir . $this->today . '.zip')) {
                $this->addResponse('Nothing to extract!', 1);

                return false;
            }

            $zip = new \ZipArchive;

            $zip->open(base_path($this->destDir . $this->today . '.zip'));

            if (!$zip->extractTo(base_path($this->destDir . $this->today))) {
                return $this->extractionFail($zip->getStatusString());
            }
        } catch (FilesystemException | UnableToCheckExistence | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        return true;
    }

    protected function extractionFail($output)
    {
        $this->addResponse('Error extracting file', 1, ['output' => $output]);

        return false;
    }

    protected function processEtfSchemesData($data = [])
    {
        $this->method = 'processEtfSchemesData';

        //Betashare Screener :
        //Is good for obtaining current ETFs. As it is where people will invest, we need to refer this first.
        //We need to convert charts to nums to store the information in our db. Example A = 1, B = 2, so 1GOV will become 171522.
        //So, the scheme ID will become 171522 for 1GOV ticker.
        //To Obtain the data from Betashare, login and go to this link:
        //https://direct.betashares.com.au/invest/screener?kind=etf&size=500
        //Under browser inspect, look for a POST request with URL: https://search.betashares.services/search
        //The response will show you all schemes. Copy the results value and paste it in a betashare.json file in DATA folder.
        //
        //Marketindex :
        //Is good for additional information like description, if needed.
        //To obtain market index details, we need to register and download their asxworkbook. It has a list of all current etfs
        //The asxworkbook is in xls format, you need to export the etf sheet into marketindex.csv and put it in DATA folder.

        //First convert Market index CSV to JSON
        try {
            $csv = Reader::createFromStream($this->localContent->readStream($this->destDir . 'marketindex.csv'));
            $csv->setHeaderOffset(0);

            $statement = (new Statement())->orderByAsc('Ticker');
            $records = $statement->process($csv);

            $totalRecords = count($records);
            $lineNo = 1;

            foreach ($records as $line) {
                if ($line['ETF'] === '' &&
                    $line['Ticker'] === '' &&
                    $line['Mngt Fee'] === '' &&
                    $line['Benchmark'] === '' &&
                    $line['Description'] === ''
                ) {
                    continue;

                    $lineNo++;
                }

                if ($line['Mngt Fee'] === '' &&
                    $line['Benchmark'] === '' &&
                    $line['Description'] === ''
                ) {
                    continue;

                    $lineNo++;
                }

                $id = $this->alphabet_to_number($line['Ticker']);

                $marketIndexEtfs[$id] = $line;

                $this->marketIndexEtfs[$id]['marketindex'] = $line;
            }
        } catch (\throwable $e) {
            $errors['exception'] = $e->getMessage();

            if (isset($lineNo)) {
                $errors['lineNo'] = $lineNo;
            }
            if (isset($line)) {
                $errors['line'] = $this->helper->encode($line);
            }
            if (isset($lineMd5)) {
                $errors['lineMd5'] = $lineMd5;
            }
            if (isset($scheme)) {
                $errors['scheme'] = $this->helper->encode($scheme);
            }

            $this->addResponse($e->getMessage(), 1, ['errors' => $errors]);

            $this->basepackages->progress->setErrors($errors);

            $this->basepackages->progress->resetProgress();

            throw $e;
        }

        if (count($this->marketIndexEtfs) === 0) {
            $this->addResponse('Unable to extract marketindex data from csv file.', 1);

            return false;
        }

        try {
            $this->localContent->write($this->destDir . 'marketindex.json', $this->helper->encode($marketIndexEtfs));
        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        //Read Betashares Etfs json file
        try {
            if ($this->localContent->fileExists($this->destDir . 'betashares.json')) {
                $betasharesEtfs = $this->helper->decode($this->localContent->read($this->destDir . 'betashares.json'), true)['results'];
            } else {
                $this->addResponse('Betashares json file does not exist!', 1);

                return false;
            }
        } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        if (count($betasharesEtfs) === 0) {
            $this->addResponse('Unable to read betashares data from json file.', 1);

            return false;
        }

        $id = null;

        foreach ($betasharesEtfs as $betashare) {
            $id = $this->alphabet_to_number($betashare['symbol']);

            $this->betasharesEtfs[$id]['betashares'] = $betashare;
        }

        $this->combinedEtfs = array_replace_recursive($this->marketIndexEtfs, $this->betasharesEtfs);

        try {
            $this->localContent->write($this->destDir . 'combined.json', $this->helper->encode($this->combinedEtfs));
        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        //Once combined.json etf information file is created, generate and update yticker file on github repository so it can extract data from Yahoo Finance.
        $yticker = [];

        foreach ($this->combinedEtfs as $etf) {
            if (isset($etf['betashares']['symbol'])) {
                if (!in_array(strtoupper($etf['betashares']['symbol']), $yticker)) {
                    array_push($yticker, strtoupper($etf['betashares']['symbol']));
                }
            } else if (isset($etf['marketindex']['Ticker'])) {
                if (!in_array(strtoupper($etf['marketindex']['Ticker']), $yticker)) {
                    array_push($yticker, strtoupper($etf['marketindex']['Ticker']));
                }
            }
        }

        try {
            $this->localContent->write($this->destDir . 'yticker.json', $this->helper->encode($yticker));
        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        $totalRecords = count($this->combinedEtfs);
        $lineNo = 1;

        try {
            foreach ($this->combinedEtfs as $schemeId => $schemeData) {
                $store = 'apps_fintech_etf_schemes_all';

                //Timer
                $this->basepackages->utils->setMicroTimer('Start');

                if (isset($schemeData['betashares']['display_name'])) {
                    $schemeName = strtolower($schemeData['betashares']['display_name']);
                } else if (isset($schemeData['marketindex']['ETF'])) {
                    $schemeName = strtolower($schemeData['marketindex']['ETF']);
                } else {
                    continue;
                }

                try {
                    $scheme = null;

                    if ($this->localContent->fileExists('.ff/sp/' . $store . '/data/' . $schemeId . '.json')) {
                        $scheme = $this->helper->decode($this->localContent->read('.ff/sp/' . $store . '/data/' . $schemeId . '.json'), true);
                    }
                    if ($this->localContent->fileExists('.ff/sp/apps_fintech_etf_schemes/data/' . $schemeId . '.json')) {
                        $scheme = $this->helper->decode($this->localContent->read('.ff/sp/' . $store . '/data/' . $schemeId . '.json'), true);
                        $store = 'apps_fintech_etf_schemes';
                    }
                } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }

                if (!$scheme) {
                    $scheme = [];
                }

                if (!isset($schemeData['betashares'])) {
                    continue;
                }

                $amc = $this->processAmcs($schemeData);
                if (!$amc) {
                    $this->basepackages->progress->setErrors([
                        'error' => 'Cannot create new AMC information for line# ' . $lineNo,
                        'line' => $schemeId
                    ]);

                    $this->addResponse('Cannot create new AMC information for line# ' . $lineNo, 1, ['line' => $schemeId]);

                    return false;
                }

                $category = $this->processCategories($schemeData);
                if (!$category) {
                    $this->basepackages->progress->setErrors([
                        'error' => 'Cannot create new category information for line# ' . $lineNo,
                        'line' => $schemeId
                    ]);

                    $this->addResponse('Cannot create new category information for line# ' . $lineNo, 1, ['line' => $schemeId]);

                    return false;
                }

                $scheme['amc_id'] = $amc['id'];
                $scheme['id'] = (int) $schemeId;
                $scheme['symbol'] = $schemeData['betashares']['symbol'];
                $scheme['scheme_type'] = 'Growth';
                $scheme['category_id'] = $category['id'];
                $scheme['name'] = $schemeData['betashares']['display_name'];
                $scheme['scheme_name'] = $schemeData['betashares']['display_name'];
                $scheme['launch_date'] = $schemeData['betashares']['inception_date'];
                $scheme['latest_nav'] = 0;
                if ($scheme['launch_date'] !== '') {
                    if (!isset($this->parsedCarbon[$scheme['launch_date']])) {
                        $this->parsedCarbon[$scheme['launch_date']] = \Carbon\Carbon::parse($scheme['launch_date']);
                    }

                    $scheme['launch_date'] = $this->parsedCarbon[$scheme['launch_date']]->toDateString();
                }
                $scheme['minimum_amount'] = null;
                $scheme['expense_ratio_type'] = 'Direct';
                $scheme['plan_type'] = 'Growth';
                $scheme['management_type'] = $schemeData['betashares']['management_approach'];
                $scheme['scheme_md5'] = null;
                $scheme['navs_last_updated'] = null;

                if ($this->config->databasetype === 'db') {
                    $this->db->insertAsDict($store, $scheme);//This also needs update.
                } else {
                    try {
                        $this->localContent->write('.ff/sp/apps_fintech_etf_schemes/data/' . $schemeId . '.json', $this->helper->encode($scheme));
                    } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }

                    try {
                        $this->localContent->write('.ff/sp/apps_fintech_etf_schemes_all/data/' . $schemeId . '.json', $this->helper->encode($scheme));
                    } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }
                }

                //Timer
                $this->processUpdateTimer($totalRecords, $lineNo);

                $lineNo++;
            }
        } catch (\throwable $e) {
            $errors['exception'] = $e->getMessage();

            if (isset($lineNo)) {
                $errors['lineNo'] = $lineNo;
            }
            if (isset($line)) {
                $errors['line'] = $this->helper->encode($line);
            }
            if (isset($lineMd5)) {
                $errors['lineMd5'] = $lineMd5;
            }
            if (isset($scheme)) {
                $errors['scheme'] = $this->helper->encode($scheme);
            }

            $this->addResponse($e->getMessage(), 1, ['errors' => $errors]);

            $this->basepackages->progress->setErrors($errors);

            $this->basepackages->progress->resetProgress();

            throw $e;
        }

        return true;
    }

    protected function processEtfNavsData($data = [])
    {
        $this->method = 'processEtfNavsData';

        try {
            if (isset($data['scheme_id'])) {
                try {
                    if ($this->localContent->fileExists('.ff/sp/apps_fintech_etf_schemes/data/' . $data['scheme_id'] . '.json')) {
                        $this->schemes = [$this->helper->decode($this->localContent->read('.ff/sp/apps_fintech_etf_schemes/data/' . $data['scheme_id'] . '.json'), true)];

                        $dbCount = 1;
                    } else {
                        if (isset($data['force']) && $data['force'] == 'true') {
                            if ($this->localContent->fileExists('.ff/sp/apps_fintech_etf_schemes_all/data/' . $data['scheme_id'] . '.json')) {
                                $this->schemes = [$this->helper->decode($this->localContent->read('.ff/sp/apps_fintech_etf_schemes_all/data/' . $data['scheme_id'] . '.json'), true)];

                                $dbCount = 1;
                            } else {
                                $this->addResponse('Scheme with ID does not exists', 1);

                                return false;
                            }
                        } else {
                            $this->addResponse('Scheme with ID does not exists', 1);

                            return false;
                        }
                    }
                } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }
            } else {
                if (!$this->schemesPackage) {
                    $this->schemesPackage = $this->usePackage(EtfSchemes::class);
                }

                $this->schemes = $this->schemesPackage->getAll()->etfschemes;

                $dbCount = count($this->schemes);

                if ($dbCount === 0) {
                    $this->addResponse('No Schemes found, Import schemes data first.', 1);

                    return false;
                }
            }

            if (count($this->schemes) > 1) {
                $this->schemes = msort($this->schemes, 'id');
            }

            //To reimport everything!! Comment if not used.
            // $data['get_all_navs'] = true;

            for ($i = 0; $i < $dbCount; $i++) {
                $this->basepackages->utils->setMicroTimer('Start');

                $etfNavsArr = null;

                try {
                    $id = $this->alphabet_to_number($this->schemes[$i]['symbol']);

                    if ($id !== $this->schemes[$i]['id'] ||
                        !$this->localContent->fileExists($this->destDir . $this->today . '/' . strtoupper($this->schemes[$i]['symbol']) . '.json')
                    ) {
                        try {
                            if ($this->localContent->fileExists('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$i]['id'] . '.json')) {
                                $this->localContent->delete('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$i]['id'] . '.json');

                                unset($this->schemes[$this->schemes[$i]['id']]);

                                $this->processUpdateTimer($dbCount, $i + 1);

                                continue;
                            }
                        } catch (FilesystemException | UnableToDeleteFile | UnableToDeleteDirectory | UnableToCheckExistence | \throwable $e) {
                            $this->addResponse($e->getMessage(), 1);

                            return false;
                        }
                    }

                    $etfNavsArr = $this->helper->decode($this->localContent->read($this->destDir . $this->today . '/' . strtoupper($this->schemes[$i]['symbol']) . '.json'), true);
                } catch (FilesystemException | UnableToCheckExistence | \throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }

                if (!$etfNavsArr ||
                    ($etfNavsArr && !isset($etfNavsArr['quote'])) ||
                    (isset($etfNavsArr['quote']) && count($etfNavsArr['quote']) === 0)
                ) {
                    try {
                        if ($this->localContent->fileExists('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$i]['id'] . '.json')) {
                            $this->localContent->delete('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$i]['id'] . '.json');

                            unset($this->schemes[$this->schemes[$i]['id']]);

                            $this->processUpdateTimer($dbCount, $i + 1);

                            continue;
                        }
                    } catch (FilesystemException | UnableToDeleteFile | UnableToDeleteDirectory | UnableToCheckExistence | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }
                }

                $etfNavsArr = array_values($etfNavsArr['quote']);

                if (($etfNavsArr && count($etfNavsArr) === 1) ||
                    (($etfNavsArr && count($etfNavsArr) <= 2) && (isset($etfNavsArr[1]['date']) && $etfNavsArr[0]['date'] === $etfNavsArr[1]['date']))
                ) {
                    try {
                        if ($this->localContent->fileExists('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$i]['id'] . '.json')) {
                            $this->localContent->delete('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$i]['id'] . '.json');

                            unset($this->schemes[$this->schemes[$i]['id']]);

                            $this->processUpdateTimer($dbCount, $i + 1);

                            continue;
                        }
                    } catch (FilesystemException | UnableToDeleteFile | UnableToDeleteDirectory | UnableToCheckExistence | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }
                }

                if (!isset($this->parsedCarbon[$this->helper->last($etfNavsArr)['date']])) {
                    $this->parsedCarbon[$this->helper->last($etfNavsArr)['date']] = \Carbon\Carbon::parse($this->helper->last($etfNavsArr)['date']);
                }

                if (!isset($this->parsedCarbon[$this->today])) {
                    $this->parsedCarbon[$this->today] = \Carbon\Carbon::parse($this->today);
                }

                $numberOfDays = $this->parsedCarbon[$this->helper->last($etfNavsArr)['date']]->diffInDays($this->parsedCarbon[$this->today]) + 1;

                //We remove the scheme if it is retired, no update from last 30 days
                //This is an assumption that we will we will be updating the scheme database everyday.
                if (!isset($data['get_all_navs']) && $numberOfDays >= 30) {
                    try {
                        if ($this->localContent->fileExists('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$i]['id'] . '.json')) {
                            $this->localContent->delete('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$i]['id'] . '.json');

                            unset($this->schemes[$this->schemes[$i]['id']]);

                            $this->processUpdateTimer($dbCount, $i + 1);

                            continue;
                        }
                    } catch (FilesystemException | UnableToDeleteFile | UnableToDeleteDirectory | UnableToCheckExistence | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }
                }

                if (!isset($this->schemes[$i]['start_date']) ||
                    (isset($this->schemes[$i]['start_date']) && $this->schemes[$i]['start_date'] != $this->helper->first($etfNavsArr)['date'])
                ) {
                    $this->schemes[$i]['start_date'] = $this->helper->first($etfNavsArr)['date'];

                    try {
                        $this->localContent->write('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$i]['id'] . '.json', $this->helper->encode($this->schemes[$i]));
                    } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }
                }

                $etfNavs = [];

                for ($etfNavsArrKey = 0; $etfNavsArrKey < count($etfNavsArr); $etfNavsArrKey++) {
                    $etfNavs[$etfNavsArr[$etfNavsArrKey]['date']] = $etfNavsArr[$etfNavsArrKey];
                }

                $dbNav = null;

                try {
                    if ($this->localContent->fileExists('.ff/sp/apps_fintech_etf_schemes_navs/data/' . $this->schemes[$i]['id'] . '.json')) {
                        $dbNav = $this->helper->decode($this->localContent->read('.ff/sp/apps_fintech_etf_schemes_navs/data/' . $this->schemes[$i]['id'] . '.json'), true);
                    }
                } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }

                if ($dbNav && $dbNav['navs'] && count($dbNav['navs']) > 0 &&
                    isset($dbNav['last_updated']) &&
                    !isset($data['get_all_navs']) &&
                    $this->helper->last($etfNavs)['date'] === $dbNav['last_updated']
                ) {
                    $this->processUpdateTimer($dbCount, $i + 1);

                    continue;
                }

                if (!$dbNav) {
                    $dbNav = [];
                    $dbNav['id'] = (int) $this->schemes[$i]['id'];
                    $dbNav['navs'] = [];
                } else {
                    if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
                        $dbNav['navs'] = [];
                    }
                }

                $firstEtfNavs = $this->helper->first($etfNavs);

                $dbNav['last_updated'] = $this->helper->last($etfNavs)['date'];

                $newNavs = false;

                if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
                    $etfNavs = array_values($etfNavs);
                } else {
                    if (count($dbNav['navs']) > 0) {
                        $etfNavsKeysDiff = array_diff(array_keys($etfNavs), array_keys($dbNav['navs']));

                        if (count($etfNavsKeysDiff) > 0) {
                            $etfNavs = array_values(array_slice($etfNavs, $this->helper->firstKey($etfNavsKeysDiff) - 2));//Get previous day for diff

                            $newNavs = [];
                        } else {
                            $etfNavs = array_values($etfNavs);
                        }
                    } else {
                        $etfNavs = array_values($etfNavs);
                    }
                }

                $etfNavs = $this->fillEtfNavDays($etfNavs, $this->schemes[$i]['id']);

                if (count($etfNavs) === 0) {
                    $this->processUpdateTimer($dbCount, $i + 1);

                    continue;
                }

                foreach ($etfNavs as $etfNavKey => $etfNav) {
                    if (!isset($dbNav['navs'][$etfNav['date']])) {
                        $dbNav['navs'][$etfNav['date']]['nav'] = $etfNav['close'];
                        $dbNav['navs'][$etfNav['date']]['date'] = $etfNav['date'];
                        if (!isset($this->parsedCarbon[$etfNav['date']])) {
                            $this->parsedCarbon[$etfNav['date']] = \Carbon\Carbon::parse($etfNav['date']);
                        }

                        $dbNav['navs'][$etfNav['date']]['timestamp'] = $this->parsedCarbon[$etfNav['date']]->timestamp;
                        $dbNav['navs'][$etfNav['date']]['diff'] = 0;
                        $dbNav['navs'][$etfNav['date']]['diff_percent'] = 0;
                        $dbNav['navs'][$etfNav['date']]['trajectory'] = '-';
                        $dbNav['navs'][$etfNav['date']]['diff_since_inception'] = 0;
                        $dbNav['navs'][$etfNav['date']]['diff_percent_since_inception'] = 0;

                        if ($etfNavKey !== 0) {
                            $previousDay = $etfNavs[$etfNavKey - 1];

                            $dbNav['navs'][$etfNav['date']]['diff'] =
                                numberFormatPrecision($etfNav['close'] - $previousDay['close'], 4);
                            $dbNav['navs'][$etfNav['date']]['diff_percent'] =
                                numberFormatPrecision(($etfNav['close'] * 100 / $previousDay['close']) - 100, 2);

                            if ($etfNav['close'] > $previousDay['close']) {
                                $dbNav['navs'][$etfNav['date']]['trajectory'] = 'up';
                            } else {
                                $dbNav['navs'][$etfNav['date']]['trajectory'] = 'down';
                            }

                            $dbNav['navs'][$etfNav['date']]['diff_since_inception'] =
                                numberFormatPrecision($etfNav['close'] - $firstEtfNavs['close'], 4);
                            $dbNav['navs'][$etfNav['date']]['diff_percent_since_inception'] =
                                numberFormatPrecision(($etfNav['close'] * 100 / $firstEtfNavs['close'] - 100), 2);
                        }

                        if ($newNavs !== false) {
                            $newNavs[$etfNav['date']] = $dbNav['navs'][$etfNav['date']];
                        }
                    }
                }

                $dbNav['navs'] = msort(array: $dbNav['navs'], key: 'timestamp', preserveKey: true);
                $newNavs = msort(array: $newNavs, key: 'timestamp', preserveKey: true);

                if (!$this->createChunks($dbNav, $data, $newNavs)) {
                    return false;
                }

                if (!$this->createRollingReturns($dbNav, $i, $data, $newNavs)) {
                    return false;
                }

                if ($this->config->databasetype === 'db') {
                    $this->db->insertAsDict('apps_fintech_etf_navs', $dbNav);
                } else {
                    try {
                        $this->localContent->write('.ff/sp/apps_fintech_etf_schemes_navs/data/' . $this->schemes[$i]['id'] . '.json', $this->helper->encode($dbNav));
                    } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }

                    $this->schemes[$i]['navs_last_updated'] = $dbNav['last_updated'];
                    $this->schemes[$i]['latest_nav'] = $this->helper->last($dbNav['navs'])['nav'];

                    try {
                        $this->localContent->write('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$i]['id'] . '.json', $this->helper->encode($this->schemes[$i]));
                    } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                        $this->addResponse($e->getMessage(), 1);

                        return false;
                    }
                }

                $this->processUpdateTimer($dbCount, $i + 1);
            }
        } catch (\throwable $e) {
            trace([$e]);
            if (isset($data['scheme_id'])) {
                $schemeId = $data['scheme_id'];
            } else {
                $schemeId = $this->schemes[$i]['id'];
            }

            $this->basepackages->progress->setErrors([
                'error'     => 'Cannot process scheme nav for scheme id# ' . $schemeId,
                'message'   => $e->getMessage()
            ]);

            $this->addResponse('Cannot process scheme nav for scheme id# ' . $schemeId, 1, ['message' => $e->getMessage()]);

            return false;
        }

        //updateinfo
        try {
            $this->localContent->write($this->destDir . 'updateinfo.json', $this->helper->encode(['updated' => $this->today]));
        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        return true;
    }

    //We also extract schemeSnapshot chunks and rollingReturns via this tools.
    public function createChunks($dbNav, $data, $newNavs = false, &$schemeSnapshot = null)
    {
        if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
            $chunks = [];
            $chunks['id'] = (int) $dbNav['id'];
            $chunks['last_updated'] = $dbNav['last_updated'];
            $chunks['navs_chunks']['all'] = $dbNav['navs'];
        } else {
            try {
                if ($this->localContent->fileExists('.ff/sp/apps_fintech_etf_schemes_navs_chunks/data/' . $dbNav['id'] . '.json')) {
                    $chunks = $this->helper->decode($this->localContent->read('.ff/sp/apps_fintech_etf_schemes_navs_chunks/data/' . $dbNav['id'] . '.json'), true);

                    if (isset($chunks['navs_chunks']['all']) && count($chunks['navs_chunks']['all']) > 0) {
                        if ($this->helper->last($chunks['navs_chunks']['all'])['date'] === $this->helper->last($dbNav['navs'])['date']) {
                            return true;
                        }
                    }
                } else {
                    $chunks = [];
                    $chunks['id'] = (int) $dbNav['id'];
                    $chunks['last_updated'] = $dbNav['last_updated'];
                }
            } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }

            if ($newNavs && count($newNavs) > 0) {
                if (isset($chunks['navs_chunks']['all']) && count($chunks['navs_chunks']['all']) > 0) {
                    $chunks['navs_chunks']['all'] = array_replace($chunks['navs_chunks']['all'], $newNavs);
                } else {
                    $chunks['navs_chunks']['all'] = $newNavs;
                }
            } else {
                if (isset($chunks['navs_chunks']['all']) && count($chunks['navs_chunks']['all']) > 0) {
                    $chunks['navs_chunks']['all'] = array_replace($chunks['navs_chunks']['all'], $dbNav['navs']);
                } else {
                    $chunks['navs_chunks']['all'] = $dbNav['navs'];
                }
            }
        }

        $datesKeys = array_keys($chunks['navs_chunks']['all']);

        foreach (['week', 'month', 'threeMonth', 'sixMonth', 'year', 'threeYear', 'fiveYear', 'tenYear', 'fifteenYear', 'twentyYear', 'twentyFiveYear', 'thirtyYear'] as $time) {
            $latestDate = \Carbon\Carbon::parse($this->helper->lastKey($chunks['navs_chunks']['all']));
            $timeDate = null;

            if ($time === 'week') {
                $timeDate = $latestDate->subDay(6)->toDateString();
            } else if ($time === 'month') {
                $timeDate = $latestDate->subMonth()->toDateString();
            } else if ($time === 'threeMonth') {
                $timeDate = $latestDate->subMonth(3)->toDateString();
            } else if ($time === 'sixMonth') {
                $timeDate = $latestDate->subMonth(6)->toDateString();
            } else if ($time === 'year') {
                $timeDate = $latestDate->subYear()->toDateString();
            } else if ($time === 'threeYear') {
                $timeDate = $latestDate->subYear(3)->toDateString();
            } else if ($time === 'fiveYear') {
                $timeDate = $latestDate->subYear(5)->toDateString();
            } else if ($time === 'tenYear') {
                $timeDate = $latestDate->subYear(10)->toDateString();
            } else if ($time === 'fifteenYear') {
                $timeDate = $latestDate->subYear(15)->toDateString();
            } else if ($time === 'twentyYear') {
                $timeDate = $latestDate->subYear(20)->toDateString();
            } else if ($time === 'twentyFiveYear') {
                $timeDate = $latestDate->subYear(25)->toDateString();
            } else if ($time === 'thirtyYear') {
                $timeDate = $latestDate->subYear(30)->toDateString();
            }

            if (isset($chunks['navs_chunks']['all'][$timeDate])) {
                $timeDateKey = array_search($timeDate, $datesKeys);
                $timeDateChunks = array_slice($chunks['navs_chunks']['all'], $timeDateKey);

                if (count($timeDateChunks) > 0) {
                    $chunks['navs_chunks'][$time] = [];

                    foreach ($timeDateChunks as $timeDateChunkDate => $timeDateChunk) {
                        $chunks['navs_chunks'][$time][$timeDateChunkDate] = [];
                        $chunks['navs_chunks'][$time][$timeDateChunkDate]['date'] = $timeDateChunk['date'];
                        $chunks['navs_chunks'][$time][$timeDateChunkDate]['nav'] = $timeDateChunk['nav'];
                        $chunks['navs_chunks'][$time][$timeDateChunkDate]['diff'] =
                            numberFormatPrecision($timeDateChunk['nav'] - $this->helper->first($timeDateChunks)['nav'], 4);
                        $chunks['navs_chunks'][$time][$timeDateChunkDate]['diff_percent'] =
                            numberFormatPrecision(($timeDateChunk['nav'] * 100 / $this->helper->first($timeDateChunks)['nav'] - 100), 2);
                    }
                }
            }
        }

        try {
            if ($schemeSnapshot) {
                $this->setModelToUse(AppsFintechEtfSchemesSnapshotsNavsChunks::class);

                if ($this->config->databasetype !== 'db') {
                    $this->ffStore = $this->ff->store($this->ffStoreToUse);

                    $this->ffStore->setValidateData(false);
                }

                $schemeSnapshot['navs_chunks_ids'][$this->helper->last($dbNav['navs'])['date']] = $this->getLastInsertedId() + 1;

                try {
                    $this->localContent->write('.ff/sp/apps_fintech_etf_schemes_snapshots_navs_chunks/data/' . $schemeSnapshot['navs_chunks_ids'][$this->helper->last($dbNav['navs'])['date']] . '.json', $this->helper->encode($chunks));

                    $this->ffStore->count(true);
                } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }

                if ($this->getLastInsertedId() !== $schemeSnapshot['navs_chunks_ids'][$this->helper->last($dbNav['navs'])['date']]) {
                    $this->addResponse('Could not insert/update snapshot navs chunks, contact developer', 1);

                    return false;
                }
            } else {
                $this->localContent->write('.ff/sp/apps_fintech_etf_schemes_navs_chunks/data/' . $chunks['id'] . '.json', $this->helper->encode($chunks));
            }
        } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
            $this->addResponse($e->getMessage(), 1);

            return false;
        }

        return true;
    }

    //We also extract schemeSnapshot chunks and rollingReturns via this tools.
    public function createRollingReturns($dbNav, $schemeId, $data, $newNavs = false, &$schemeSnapshot = null)
    {
        if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
            $rr = [];
            $rr['id'] = $dbNav['id'];
            $rr['last_updated'] = $dbNav['last_updated'];
        } else {
            try {
                if ($this->localContent->fileExists('.ff/sp/apps_fintech_etf_schemes_navs_rolling_returns/data/' . $dbNav['id'] . '.json')) {
                    $rr = $this->helper->decode($this->localContent->read('.ff/sp/apps_fintech_etf_schemes_navs_rolling_returns/data/' . $dbNav['id'] . '.json'), true);

                    if (isset($rr['year']) && count($rr['year']) > 0) {
                        if ($this->helper->last($rr['year'])['to'] === $this->helper->last($dbNav['navs'])['date']) {
                            return true;
                        }
                    }
                } else {
                    $rr = [];
                    $rr['id'] = $dbNav['id'];
                    $rr['last_updated'] = $dbNav['last_updated'];
                }
            } catch (FilesystemException | UnableToReadFile | UnableToCheckExistence | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }
        }

        $schemeRr = [];
        $schemeRr['day_cagr'] = $this->helper->last($dbNav['navs'])['diff_percent'];
        $schemeRr['day_trajectory'] = $this->helper->last($dbNav['navs'])['trajectory'];

        foreach (['year', 'two_year', 'three_year', 'five_year', 'seven_year', 'ten_year', 'fifteen_year', 'twenty_year', 'twenty_five_year', 'thirty_year'] as $rrTerm) {
            $schemeRr[$rrTerm . '_rr'] = null;
            $schemeRr[$rrTerm . '_cagr'] = null;
        }

        if ($schemeSnapshot) {
            $schemeSnapshot['snapshots'][$this->helper->last($dbNav['navs'])['date']] = $schemeRr;
        } else {
            $this->schemes[$schemeId] = array_replace($this->schemes[$schemeId], $schemeRr);
            try {
                $this->localContent->write('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$schemeId]['id'] . '.json', $this->helper->encode($this->schemes[$schemeId]));
            } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }
        }

        $latestDate = \Carbon\Carbon::parse($this->helper->lastKey($dbNav['navs']));
        $yearBefore = $latestDate->subYear()->toDateString();

        if (!isset($dbNav['navs'][$yearBefore])) {
            if ($schemeSnapshot) {
                $this->setModelToUse(AppsFintechEtfSchemesSnapshotsNavsRollingReturns::class);

                if ($this->config->databasetype !== 'db') {
                    $this->ffStore = $this->ff->store($this->ffStoreToUse);

                    $this->ffStore->setValidateData(false);
                }

                $schemeSnapshot['rolling_returns_ids'][$this->helper->last($dbNav['navs'])['date']] = $this->getLastInsertedId() + 1;

                try {
                    $this->localContent->write('.ff/sp/apps_fintech_etf_schemes_snapshots_navs_rolling_returns/data/' . $schemeSnapshot['rolling_returns_ids'][$this->helper->last($dbNav['navs'])['date']] . '.json', $this->helper->encode($rr));

                    $this->ffStore->count(true);
                } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }

                if ($this->getLastInsertedId() !== $schemeSnapshot['rolling_returns_ids'][$this->helper->last($dbNav['navs'])['date']]) {
                    $this->addResponse('Could not insert/update snapshot rolling returns, contact developer', 1);

                    return false;
                }
            } else {
                try {
                    $this->localContent->write('.ff/sp/apps_fintech_etf_schemes_navs_rolling_returns/data/' . $rr['id'] . '.json', $this->helper->encode($rr));
                } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }
            }

            return true;
        }

        if (isset($data['get_all_navs']) && $data['get_all_navs'] == 'true') {
            $dbNavNavs = $dbNav['navs'];
        } else {
            if ($newNavs && count($newNavs) > 0) {
                $dbNavNavs = $newNavs;

                foreach ($dbNavNavs as $date => $nav) {
                    foreach (['year', 'two_year', 'three_year', 'five_year', 'seven_year', 'ten_year', 'fifteen_year', 'twenty_year', 'twenty_five_year', 'thirty_year'] as $rrTerm) {
                        try {
                            $toDate = \Carbon\Carbon::parse($date);

                            if ($rrTerm === 'year') {
                                $fromDate = $toDate->subYear()->toDateString();
                            } else if ($rrTerm === 'two_year') {
                                $fromDate = $toDate->subYear(2)->toDateString();
                            } else if ($rrTerm === 'three_year') {
                                $fromDate = $toDate->subYear(3)->toDateString();
                            } else if ($rrTerm === 'five_year') {
                                $fromDate = $toDate->subYear(5)->toDateString();
                            } else if ($rrTerm === 'seven_year') {
                                $fromDate = $toDate->subYear(7)->toDateString();
                            } else if ($rrTerm === 'ten_year') {
                                $fromDate = $toDate->subYear(10)->toDateString();
                            } else if ($rrTerm === 'fifteen_year') {
                                $fromDate = $toDate->subYear(15)->toDateString();
                            } else if ($rrTerm === 'twenty_year') {
                                $fromDate = $toDate->subYear(20)->toDateString();
                            } else if ($rrTerm === 'twenty_five_year') {
                                $fromDate = $toDate->subYear(25)->toDateString();
                            } else if ($rrTerm === 'thirty_year') {
                                $fromDate = $toDate->subYear(30)->toDateString();
                            }

                            if (isset($dbNav['navs'][$fromDate])) {
                                $dbNavNavs[$fromDate] = $dbNav['navs'][$fromDate];
                            }
                        } catch (\throwable $e) {
                            $this->addResponse($e->getMessage(), 1);

                            return false;
                        }
                    }
                }

                $dbNavNavs = msort(array: $dbNavNavs, key: 'timestamp', preserveKey: true);
            } else {
                $dbNavNavs = $dbNav['navs'];
            }
        }

        $processingYear = null;
        $nationalHolidays = [];

        foreach ($dbNavNavs as $date => $nav) {
            foreach (['year', 'two_year', 'three_year', 'five_year', 'seven_year', 'ten_year', 'fifteen_year', 'twenty_year', 'twenty_five_year', 'thirty_year'] as $rrTerm) {
                try {
                    $fromDate = \Carbon\Carbon::parse($date);

                    if ($fromDate->isWeekend()) {
                        continue;
                    }

                    if (!$processingYear) {
                        $processingYear = $fromDate->year;
                    }

                    if ($processingYear !== $fromDate->year) {
                        $processingYear = $fromDate->year;

                        $this->getNationalHolidays($nationalHolidays, $processingYear);
                    } else {
                        if (!isset($nationalHolidays[$processingYear])) {
                            $this->getNationalHolidays($nationalHolidays, $processingYear);
                        }
                    }

                    if (in_array($date, $nationalHolidays[$processingYear])) {
                        continue;
                    }

                    $time = null;

                    if ($rrTerm === 'year') {
                        $toDate = $fromDate->addYear()->toDateString();
                        $time = 1;
                    } else if ($rrTerm === 'two_year') {
                        $toDate = $fromDate->addYear(2)->toDateString();
                        $time = 2;
                    } else if ($rrTerm === 'three_year') {
                        $toDate = $fromDate->addYear(3)->toDateString();
                        $time = 3;
                    } else if ($rrTerm === 'five_year') {
                        $toDate = $fromDate->addYear(5)->toDateString();
                        $time = 5;
                    } else if ($rrTerm === 'seven_year') {
                        $toDate = $fromDate->addYear(7)->toDateString();
                        $time = 7;
                    } else if ($rrTerm === 'ten_year') {
                        $toDate = $fromDate->addYear(10)->toDateString();
                        $time = 10;
                    } else if ($rrTerm === 'fifteen_year') {
                        $toDate = $fromDate->addYear(15)->toDateString();
                        $time = 15;
                    } else if ($rrTerm === 'twenty_year') {
                        $toDate = $fromDate->addYear(20)->toDateString();
                        $time = 20;
                    } else if ($rrTerm === 'twenty_five_year') {
                        $toDate = $fromDate->addYear(25)->toDateString();
                        $time = 25;
                    } else if ($rrTerm === 'thirty_year') {
                        $toDate = $fromDate->addYear(30)->toDateString();
                        $time = 30;
                    }

                    if (isset($rr[$rrTerm][$date])) {
                        continue;
                    }

                    if (isset($dbNavNavs[$toDate])) {
                        if (!isset($rr[$rrTerm])) {
                            $rr[$rrTerm] = [];
                        }

                        $rr[$rrTerm][$date]['from'] = $date;
                        $rr[$rrTerm][$date]['to'] = $toDate;
                        $rr[$rrTerm][$date]['cagr'] =
                            numberFormatPrecision((pow(($dbNavNavs[$toDate]['nav']/$nav['nav']),(1/$time)) - 1) * 100);

                        if ($toDate === $this->helper->last($dbNavNavs)['date']) {
                            $schemeRr[$rrTerm . '_cagr'] = $rr[$rrTerm][$date]['cagr'];
                        }
                    }
                } catch (\throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }
            }
        }

        if ($schemeSnapshot) {
            $this->setModelToUse(AppsFintechEtfSchemesSnapshotsNavsRollingReturns::class);

            if ($this->config->databasetype !== 'db') {
                $this->ffStore = $this->ff->store($this->ffStoreToUse);

                $this->ffStore->setValidateData(false);
            }

            $schemeSnapshot['rolling_returns_ids'][$this->helper->last($dbNav['navs'])['date']] = $this->getLastInsertedId() + 1;

            try {
                $this->localContent->write('.ff/sp/apps_fintech_etf_schemes_snapshots_navs_rolling_returns/data/' . $schemeSnapshot['rolling_returns_ids'][$this->helper->last($dbNav['navs'])['date']] . '.json', $this->helper->encode($rr));

                $this->ffStore->count(true);
            } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }

            if ($this->getLastInsertedId() !== $schemeSnapshot['rolling_returns_ids'][$this->helper->last($dbNav['navs'])['date']]) {
                $this->addResponse('Could not insert/update snapshot rolling returns, contact developer', 1);

                return false;
            }
        } else {
            try {
                $this->localContent->write('.ff/sp/apps_fintech_etf_schemes_navs_rolling_returns/data/' . $rr['id'] . '.json', $this->helper->encode($rr));
            } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                $this->addResponse($e->getMessage(), 1);

                return false;
            }
        }

        //Calculate RR Average for timeframes. This will be used to narrow down our fund search.
        $rrCagrs = [];
        foreach ($rr as $rrTermType => $rrTermArr) {
            if (is_array($rrTermArr)) {
                foreach ($rrTermArr as $rrTermArrDate => $rrTermArrValue) {
                    if (!isset($rrCagrs[$rrTermType])) {
                        $rrCagrs[$rrTermType] = [];
                    }

                    $rrCagrs[$rrTermType][$rrTermArrDate] = $rrTermArrValue['cagr'];
                }
            }
        }
        if (count($rrCagrs) > 0) {
            foreach ($rrCagrs as $rrCagrTerm => $rrCagrArr) {
                $schemeRr[$rrCagrTerm . '_rr'] = numberFormatPrecision(\MathPHP\Statistics\Average::mean($rrCagrArr), 2);
            }
        }

        if (count($schemeRr) > 0) {
            if ($schemeSnapshot) {
                $schemeSnapshot['snapshots'][$this->helper->last($dbNav['navs'])['date']] = $schemeRr;
            } else {
                $this->schemes[$schemeId] = array_replace($this->schemes[$schemeId], $schemeRr);

                try {
                    $this->localContent->write('.ff/sp/apps_fintech_etf_schemes/data/' . $this->schemes[$schemeId]['id'] . '.json', $this->helper->encode($this->schemes[$schemeId]));
                } catch (FilesystemException | UnableToWriteFile | \throwable $e) {
                    $this->addResponse($e->getMessage(), 1);

                    return false;
                }
            }
        }

        return true;
    }

    protected function getNationalHolidays(&$nationalHolidays, $processingYear)
    {
        $nationalHolidays[$processingYear] = [];

        $geoHolidays = $this->basepackages->geoHolidays->getNationalHolidays(null, $processingYear);

        if (count($geoHolidays) > 0) {
            foreach ($geoHolidays as $holiday) {
                if (!isset($this->parsedCarbon[$holiday['date']])) {
                    $this->parsedCarbon[$holiday['date']] = \Carbon\Carbon::parse($holiday['date']);
                }

                if ((\Carbon\Carbon::parse($holiday['date']))->isWeekend()) {
                    continue;
                }

                array_push($nationalHolidays[$processingYear], $holiday['date']);
            }
        }

        $commonPublicHolidays = ['01' => '26', '08' => '15', '10' => '02', '12' => '25'];//Holidays that repeat on the same day every year!
        array_walk($commonPublicHolidays, function($date, $month) use (&$nationalHolidays, $processingYear) {
            if (!in_array($processingYear . '-' . $month . '-' . $date, $nationalHolidays[$processingYear])) {
                array_push($nationalHolidays[$processingYear], $processingYear . '-' . $month . '-' . $date);
            }
        });
    }

    protected function fillEtfNavDays($etfNavsArr, $aetfiCode)
    {
        if (!isset($this->parsedCarbon[$this->helper->first($etfNavsArr)['date']])) {
            $this->parsedCarbon[$this->helper->first($etfNavsArr)['date']] = \Carbon\Carbon::parse($this->helper->first($etfNavsArr)['date']);
        }
        if (!isset($this->parsedCarbon[$this->helper->last($etfNavsArr)['date']])) {
            $this->parsedCarbon[$this->helper->last($etfNavsArr)['date']] = \Carbon\Carbon::parse($this->helper->last($etfNavsArr)['date']);
        }

        //Include last day in calculation
        $numberOfDays = $this->parsedCarbon[$this->helper->first($etfNavsArr)['date']]->diffInDays($this->parsedCarbon[$this->helper->last($etfNavsArr)['date']]) + 1;

        if ($numberOfDays != count($etfNavsArr)) {
            $etfNavs = [];

            foreach ($etfNavsArr as $etfNavKey => $etfNav) {
                $etfNavs[$etfNav['date']] = $etfNav;

                if (isset($etfNavsArr[$etfNavKey + 1])) {
                    $currentDate = \Carbon\Carbon::parse($etfNav['date']);
                    if (!isset($this->parsedCarbon[$etfNavsArr[$etfNavKey + 1]['date']])) {
                        $this->parsedCarbon[$etfNavsArr[$etfNavKey + 1]['date']] = \Carbon\Carbon::parse($etfNavsArr[$etfNavKey + 1]['date']);
                    }
                    $nextDate = $this->parsedCarbon[$etfNavsArr[$etfNavKey + 1]['date']];
                    $differenceDays = $currentDate->diffInDays($nextDate);

                    if ($differenceDays > 1) {
                        for ($days = 1; $days < $differenceDays; $days++) {
                            $missingDay = $currentDate->addDay(1)->toDateString();

                            if (!isset($etfNavs[$missingDay])) {
                                $etfNav['date'] = $missingDay;

                                $etfNavs[$etfNav['date']] = $etfNav;
                            }
                        }
                    }
                }
            }

            if ($numberOfDays != count($etfNavs)) {
                throw new \Exception('Cannot process missing AMFI navs correctly for aetfiCode : ' . $aetfiCode);
            }

            return array_values($etfNavs);
        }

        return $etfNavsArr;
    }

    protected function reIndexEtfSchemesData()
    {
        $this->method = 'reIndexEtfSchemesData';

        $data = [];
        $data['task'] = 're-index';
        $data['selectedStores'] = ['apps_fintech_etf_schemes'];

        $reindex = $this->core->maintainFf($data);

        if (!$reindex) {
            $this->addResponse(
                $this->core->packagesData->responseMessage,
                $this->core->packagesData->responseCode
            );

            return false;
        }

        return true;
    }

    protected function recalculatePortfolios()
    {
        $this->method = 'recalculatePortfolios';

        $portfoliosPackage = $this->usePackage(EtfPortfolios::class);

        $portfolios = $portfoliosPackage->getAll()->etfportfolios;

        if ($portfolios && count($portfolios) > 0) {
            $totalRecords = count($portfolios);
            $lineNo = 1;

            foreach ($portfolios as $portfolio) {
                $this->processUpdateTimer($totalRecords, $lineNo, 'Recalculating ' . $portfolio['name'] . '...');

                $portfoliosPackage = $this->usePackage(EtfPortfolios::class);

                if (!$portfoliosPackage->recalculatePortfolio(['portfolio_id' => $portfolio['id']])) {
                    if (str_contains($portfoliosPackage->packagesData->responseMessage, 'no transactions')) {
                        $lineNo++;

                        continue;
                    }

                    $this->addResponse(
                        $portfoliosPackage->packagesData->responseMessage,
                        $portfoliosPackage->packagesData->responseCode,
                        $portfoliosPackage->packagesData->responseData ?? []
                    );

                    return false;
                }

                $lineNo++;
            }
        }

        return true;
    }

    protected function processUpdateTimer($totalRecords, $lineNo, $text = null)
    {
        $this->basepackages->utils->setMicroTimer('End');

        $time = $this->basepackages->utils->getMicroTimer();

        if ($time && isset($time[1]['difference']) && $time[1]['difference'] !== 0) {
            $totalTime = date("H:i:s", floor($time[1]['difference'] * ($totalRecords - $lineNo)));
        } else {
            $totalTime = date("H:i:s", 0);
        }

        $this->basepackages->utils->resetMicroTimer();

        if (PHP_SAPI !== 'cli') {
            $this->basepackages->progress->updateProgress(
                method: $this->method,
                counters: ['stepsTotal' => $totalRecords, 'stepsCurrent' => $lineNo],
                text: $text ?? 'Time remaining : ' . $totalTime . '...'
            );
        }
    }

    public function sync($data)
    {
        if ($data['sync'] === 'holidays') {
            return $this->processBankHolidays($data);
        }

        $this->addResponse('Error processing sync', 1);

        return false;
    }

    protected function processAmcs(array $data)
    {
        if (!isset($data['betashares']['issuer'])) {
            return false;
        }

        if (isset($this->amcs[$data['betashares']['issuer']])) {
            return $this->amcs[$data['betashares']['issuer']];
        }

        if (!$this->amcsPackage) {
            $this->amcsPackage = new EtfAmcs;
        }

        $amc = $this->amcsPackage->getEtfAmcByName($data['betashares']['issuer']);

        if (!$amc) {
            $amc = [];
            $amc['name'] = $data['betashares']['issuer'];
            $amc['turn_around_time'] = null;

            $amc = $this->amcsPackage->addEtfAmcs($amc);

            if ($amc) {
                $amc = [];
                $amc = $this->amcsPackage->packagesData->last;
            }
        }

        $this->amcs[$data['betashares']['issuer']] = $amc;

        return $amc;
    }

    protected function processCategories(array $data)
    {
        if (!$this->categoriesPackage) {
            $this->categoriesPackage = new EtfCategories;
        }

        $categories[0] = 'General';

        if (isset($data['betashares']['asset_classes'])) {
            if (count($data['betashares']['asset_classes']) > 0) {
                $categories[0] = implode(':', $data['betashares']['asset_classes']);
            } else {
                if (isset($data['betashares']['categories'][0])) {
                    $category = strtolower($data['betashares']['categories'][0]);
                    if (str_contains('australian', $category)) {
                        $categories[0] = 'Australian';
                    } else if (str_contains('international', $category)) {
                        $categories[0] = 'International';
                    }
                }
            }
        }

        $categories[1] = 'General';

        if (isset($data['betashares']['categories'][0])) {
            $categories[1] = implode(':', $data['betashares']['categories']);
        }

        if (isset($this->categories[$categories[1]])) {
            return $this->categories[$categories[1]];
        }

        $parentCategory = $this->categoriesPackage->getEtfCategoryByName($categories[0]);

        if (!$parentCategory) {
            $parentCategory = [];
            $parentCategory['name'] = $categories[0];

            $this->categoriesPackage->addEtfCategories($parentCategory);

            $parentCategory = $this->categoriesPackage->packagesData->last;
        }

        $childCategory = $this->categoriesPackage->getEtfCategoryByName($categories[1]);

        if (!$childCategory) {
            $childCategory = [];
            $childCategory['name'] = $categories[1];
            $childCategory['parent_id'] = $parentCategory['id'];

            $this->categoriesPackage->addEtfCategories($childCategory);

            $childCategory = $this->categoriesPackage->packagesData->last;
        }

        $this->categories[$categories[1]] = $childCategory;

        return $childCategory;
    }

    protected function processBankHolidays($data)
    {
        //Regardless of where you get the data from, verification is required.
        if ($data['source'] === 'timeandate') {
            //Time and date has correct holidays for national holidays, grab those!
            //https://www.timeanddate.com/holidays/australia/2025?hol=1
        }

        $this->addResponse('Imported holiday information via ' . $data['source'] . ' successfully');

        return true;
    }

    protected function initApi($data, $sink = null, $method = null)
    {
        if ($this->apiClient && $this->apiClientConfig) {
            return true;
        }

        if (!isset($data['api_id'])) {
            $this->addResponse('API information not provided', 1, []);

            return false;
        }

        if (isset($data['api_id']) && $data['api_id'] == '0') {
            $this->addResponse('This is local module and not remote module, cannot sync.', 1, []);

            return false;
        }

        if ($sink & $method) {
            $this->apiClient = $this->basepackages->apiClientServices->setHttpOptions(['timeout' => 3600])->setMonitorProgress($sink, $method)->useApi($data['api_id']);
        } else {
            $this->apiClient = $this->basepackages->apiClientServices->useApi($data['api_id']);
        }

        $this->apiClientConfig = $this->apiClient->getApiConfig();

        if ($this->apiClientConfig['auth_type'] === 'auth' &&
            ((!$this->apiClientConfig['username'] || $this->apiClientConfig['username'] === '') &&
            (!$this->apiClientConfig['password'] || $this->apiClientConfig['password'] === ''))
        ) {
            $this->addResponse('Username/Password missing, cannot sync', 1);

            return false;
        } else if ($this->apiClientConfig['auth_type'] === 'access_token' &&
                  (!$this->apiClientConfig['access_token'] || $this->apiClientConfig['access_token'] === '')
        ) {
            $this->addResponse('Access token missing, cannot sync', 1);

            return false;
        } else if ($this->apiClientConfig['auth_type'] === 'autho' &&
                  (!$this->apiClientConfig['authorization'] || $this->apiClientConfig['authorization'] === '')
        ) {
            $this->addResponse('Authorization token missing, cannot sync', 1);

            return false;
        }

        return true;
    }

    public function getAvailableApis($getAll = false, $returnApis = true)
    {
        $apisArr = [];

        if (!$getAll) {
            $package = $this->getPackage();
            if (isset($package['settings']) &&
                isset($package['settings']['api_clients']) &&
                is_array($package['settings']['api_clients']) &&
                count($package['settings']['api_clients']) > 0
            ) {
                foreach ($package['settings']['api_clients'] as $key => $clientId) {
                    $client = $this->basepackages->apiClientServices->getApiById($clientId);

                    if ($client) {
                        array_push($apisArr, $client);
                    }
                }
            }
        } else {
            $apisArr = $this->basepackages->apiClientServices->getApiByAppType();
            $apisArr = array_merge($apisArr, $this->basepackages->apiClientServices->getApiByAppType('core'));
        }

        if (count($apisArr) > 0) {
            foreach ($apisArr as $apisArrKey => $api) {
                if ($api['category'] === 'repos' || $api['category'] === 'providers') {
                    $useApi = $this->basepackages->apiClientServices->useApi([
                            'config' =>
                                [
                                    'id'           => $api['id'],
                                    'category'     => $api['category'],
                                    'provider'     => $api['provider'],
                                    'checkOnly'    => true//Set this to check if the API exists and can be instantiated.
                                ]
                        ]);

                    if ($useApi) {
                        $apiConfig = $useApi->getApiConfig();

                        if (isset($apiConfig['repo_url']) && !str_contains($apiConfig['repo_url'], 'sp-fintech-mutualfunds')) {
                            unset($apisArr[$apisArrKey]);

                            continue;
                        }

                        $apis[$api['id']]['id'] = $apiConfig['id'];
                        $apis[$api['id']]['name'] = $apiConfig['name'];
                        if (isset($apiConfig['repo_url'])) {
                            $apis[$api['id']]['data']['url'] = $apiConfig['repo_url'];
                        } else if (isset($apiConfig['api_url'])) {
                            $apis[$api['id']]['data']['url'] = $apiConfig['api_url'];
                        }
                    }
                }
            }
        }

        if ($returnApis) {
            return $apis ?? [];
        }

        return $apisArr;
    }
}