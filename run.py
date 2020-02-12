#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from faostat import generate_datasets_and_showcases, get_indicatortypesdata, get_countries

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-faostat'

def main():
    """Generate dataset and create it in HDX"""

    filelist_url = Configuration.read()['filelist_url']
    country_group_url = Configuration.read()['country_group_url']
    dataset_codes = Configuration.read()['dataset_codes']
    showcase_base_url = Configuration.read()['showcase_base_url']
    with temp_dir('faostat') as folder:
        with Download() as downloader:
            indicatortypes = get_indicatortypesdata(filelist_url, downloader)
            countriesdata = get_countries(country_group_url, downloader)
            logger.info('Number of indicator types to upload: %d' % len(dataset_codes))
            for dataset_code in dataset_codes:
                datasets, showcases = generate_datasets_and_showcases(downloader, folder, dataset_codes[dataset_code],
                                                                      indicatortypes[dataset_code], countriesdata,
                                                                      showcase_base_url)
                logger.info('Number of datasets to upload: %d' % len(datasets))
                for i, dataset in enumerate(datasets):
                    logger.info('Creating dataset: %s' % dataset['title'])
                    dataset.preview_off()
                    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: FAOStat')
                    showcase = showcases[i]
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
