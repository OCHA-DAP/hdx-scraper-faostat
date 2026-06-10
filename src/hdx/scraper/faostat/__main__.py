#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os.path import exists, expanduser, join
from shutil import rmtree

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.faostat.pipeline import (
    download_indicatorsets,
    generate_dataset_and_showcase,
    get_countries,
)

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-faostat"
_SAVED_DATA_DIR = "saved_data"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    filelist_url = configuration["filelist_url"]
    categories = configuration["categories"]
    showcase_base_url = configuration["showcase_base_url"]
    with Download() as downloader:
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info["folder"]
            batch = info["batch"]
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=folder,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=folder,
                save=save,
                use_saved=use_saved,
            )
            indicatorsets = download_indicatorsets(
                filelist_url, categories, retriever, folder
            )
            logger.info(f"Number of categories to upload: {len(categories)}")
            countries, countrymapping = get_countries(
                script_dir_plus_file(
                    join("config", "FAOSTAT_CountryGroups.csv"),
                    main,
                ),
                retriever,
            )
            logger.info(f"Number of countries to upload: {len(countries)}")
            #            log_latest_dates(indicatorsets, [x["countrycode"] for x in countries])
            for info, country in progress_storing_folder(info, countries, "iso3"):
                for categoryname in indicatorsets:
                    (
                        dataset,
                        showcase,
                    ) = generate_dataset_and_showcase(
                        categoryname,
                        categories,
                        indicatorsets,
                        country,
                        countrymapping,
                        showcase_base_url,
                        filelist_url,
                        retriever,
                        info["folder"],
                    )
                    if dataset:
                        dataset.update_from_yaml(
                            path=script_dir_plus_file(
                                join("config", "hdx_dataset_static.yaml"),
                                main,
                            )
                        )
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            updated_by_script="HDX Scraper: FAOStat",
                            batch=batch,
                        )
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)
            for rows in indicatorsets.values():
                for row in rows:
                    split_dir = row.get("split_dir")
                    if split_dir and exists(split_dir):
                        rmtree(split_dir)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
