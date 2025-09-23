<?php

namespace Apps\Fintech\Packages\Etf\Tools\Extractdata;

use System\Base\BasePackage;

class Settings extends BasePackage
{
    public function afterUpdate($packageClass, $package, $data)
    {
        if (is_string($package['settings'])) {
            $package['settings'] = $this->helper->decode($package['settings'], true);
        }

        $apiClientServices = $this->basepackages->apiClientServices->getApiByAppType();

        if (count($package['settings']['api_clients']) > 0) {
            foreach ($apiClientServices as $api) {
                if ($api['used_by'] === null) {
                    $api['used_by'] = [];
                }

                if (is_string($api['used_by']) && $api['used_by'] !== '') {
                    $api['used_by'] = $this->helper->decode($api['used_by'], true);
                }

                if (count($api['used_by']) === 0) {
                    if (in_array($api['id'], $package['settings']['api_clients'])) {
                        $api['used_by'] = ['extractdata'];
                        $api['in_use'] = 1;
                    }
                } else {
                    if (!in_array($api['id'], $package['settings']['api_clients'])) {
                        if (in_array('extractdata', $api['used_by'])) {
                            $key = array_search('extractdata', $api['used_by']);
                            unset($api['used_by'][$key]);
                        }
                        if (count($api['used_by']) === 0) {
                            $api['in_use'] = 0;
                        }
                    } else if (in_array($api['id'], $package['settings']['api_clients'])) {
                        if (!in_array('extractdata', $api['used_by'])) {
                            array_push($api['used_by'], 'extractdata');
                            $api['in_use'] = 1;
                        }
                    }
                }

                $this->basepackages->apiClientServices->updateApi($api);
            }
        } else {
            foreach ($apiClientServices as $api) {
                if ($api['used_by'] === null) {
                    $api['used_by'] = [];
                }

                if (is_string($api['used_by']) && $api['used_by'] !== '') {
                    $api['used_by'] = $this->helper->decode($api['used_by'], true);
                }

                if (in_array('extractdata', $api['used_by'])) {
                    $key = array_search('extractdata', $api['used_by']);
                    unset($api['used_by'][$key]);
                }

                if (count($api['used_by']) === 0) {
                    $api['in_use'] = 0;
                }

                $this->basepackages->apiClientServices->updateApi($api);
            }
        }
    }
}