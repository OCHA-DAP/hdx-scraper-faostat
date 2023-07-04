#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from faostat import download_indicatorsets, generate_dataset_and_showcase, get_countries
from hdx.api.configuration import Configuration
from hdx.facades.simple import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-faostat"


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    filelist_url = configuration["filelist_url"]
    countrygroup_url = configuration["countrygroup_url"]
    categories = configuration["categories"]
    showcase_base_url = configuration["showcase_base_url"]
    with Download() as downloader:
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info["folder"]
            batch = info["batch"]
            indicatorsets = download_indicatorsets(
                filelist_url, categories, downloader, folder
            )
            logger.info(
                f"Number of categories to upload: {len(categories)}"
            )
            countries, countrymapping = get_countries(countrygroup_url, downloader)
            logger.info(f"Number of countries to upload: {len(countries)}")
            for info, country in progress_storing_folder(info, countries, "iso3"):
                for categoryname in indicatorsets:
                    (
                        dataset,
                        showcase,
                        bites_disabled,
                        qc_indicators,
                    ) = generate_dataset_and_showcase(
                        categoryname,
                        categories,
                        indicatorsets,
                        country,
                        countrymapping,
                        showcase_base_url,
                        filelist_url,
                        downloader,
                        info["folder"],
                    )
                    if dataset:
                        dataset.update_from_yaml()
                        dataset.generate_quickcharts(
                            -1, bites_disabled=bites_disabled, indicators=qc_indicators
                        )
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            hxl_update=False,
                            updated_by_script="HDX Scraper: FAOStat",
                            batch=batch,
                        )
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
