#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import wheretostart_tempdir_batch, progress_storing_folder

from faostat import generate_dataset_and_showcase, download_indicatorsets, get_countries

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-faostat'


def main():
    """Generate dataset and create it in HDX"""

    filelist_url = Configuration.read()['filelist_url']
    countrygroup_url = Configuration.read()['countrygroup_url']
    indicatorsetnames = Configuration.read()['indicatorsetnames']
    showcase_base_url = Configuration.read()['showcase_base_url']
    with Download() as downloader:
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info['folder']
            batch = info['batch']
            indicatorsets = download_indicatorsets(filelist_url, indicatorsetnames, downloader, folder)
            logger.info('Number of indicator types to upload: %d' % len(indicatorsetnames))
            countries, countrymapping = get_countries(countrygroup_url, downloader)
            logger.info('Number of countries to upload: %d' % len(countries))
            for info, country in progress_storing_folder(info, countries, 'iso3'):
                for indicatorsetname in indicatorsets:
                    dataset, showcase, bites_disabled, qc_indicators = generate_dataset_and_showcase(
                        indicatorsetname, indicatorsets, country, countrymapping, showcase_base_url, filelist_url,
                        downloader, info['folder'])
                    if dataset:
                        dataset.update_from_yaml()
                        dataset.generate_resource_view(-1, bites_disabled=bites_disabled, indicators=qc_indicators)
                        dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: FAOStat', batch=batch)
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
