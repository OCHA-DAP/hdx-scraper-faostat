#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.facades import logging_kwargs
from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from faostat import generate_datasets_and_showcases, get_indicatortypesdata, get_countriesdata

logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    filelist_url = Configuration.read()['filelist_url']
    country_group_url = Configuration.read()['country_group_url']
    dataset_codes = Configuration.read()['dataset_codes']
    showcase_base_url = Configuration.read()['showcase_base_url']
    with Download() as downloader:
        indicatortypes = get_indicatortypesdata(filelist_url, downloader)
        countriesdata = get_countriesdata(country_group_url, downloader)
        logger.info('Number of indicator types to upload: %d' % len(dataset_codes))
        for dataset_code in dataset_codes:
            datasets, showcases = generate_datasets_and_showcases(downloader, dataset_codes[dataset_code],
                                                                  indicatortypes[dataset_code], countriesdata,
                                                                  showcase_base_url)
            logger.info('Number of datasets to upload: %d' % len(datasets))
            for i, dataset in enumerate(datasets):
                logger.info('Creating dataset: %s' % dataset['title'])
                dataset.preview_off()
                dataset.create_in_hdx()
                showcase = showcases[i]
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'),
           user_agent_lookup='hdxscraper-faostat', project_config_yaml=join('config', 'project_configuration.yml'))
