#!/usr/bin/python
"""
Unit tests for FAOSTAT.

"""

import shutil
from os.path import basename, join
from pathlib import Path

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import DownloadError
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.faostat.pipeline import (
    download_indicatorsets,
    generate_dataset_and_showcase,
    get_countries,
)


class TestFaostat:
    country = {
        "countrycode": "2",
        "countryname": "Afghanistan",
        "iso3": "AFG",
        "origname": "Afghanistan",
    }
    countrymapping = {"2": ("AFG", "Afghanistan")}
    fsurl = "https://lala/Food_Security_Data_E_All_Data_(Normalized).zip"
    indicatorsets = {
        "Food Security and Nutrition": [
            {
                "DatasetCode": "FS",
                "DatasetName": "Food Security and Nutrition: Suite of Food Security Indicators",
                "Topic": "See attached document which lists sector coverage with the respective indicator.",
                "DatasetDescription": "For detailed description of the indicators below see attached document: Average Dietary Supply Adequacy;...",
                "Contact": "Carlo Cafiero",
                "Email": "Food-Security-Statistics@FAO.org",
                "DateUpdate": "2018-10-16",
                "CompressionFormat": "zip",
                "FileType": "csv",
                "FileSize": "681KB",
                "FileRows": 70890,
                "FileLocation": fsurl,
            }
        ]
    }

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=Path("src")
            / "hdx"
            / "scraper"
            / "faostat"
            / "config"
            / "project_configuration.yaml",
        )
        Locations.set_validlocations(
            [{"name": "afg", "title": "Afghanistan"}]
        )  # add locations used in tests
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = {}
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": "food security"},
                {"name": "indicators"},
                {"name": "nutrition"},
            ],
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

    @pytest.fixture(scope="function")
    def mock_urlretrieve(self):
        def myurlretrieve(url, path):
            class Headers:
                @staticmethod
                def get_content_type():
                    return "application/x-zip-compressed"

            shutil.copyfile(join("tests", "fixtures", basename(path)), path)
            return path, Headers()

        return myurlretrieve

    @pytest.fixture(scope="function")
    def retriever(self):
        class MockDownloader:
            @staticmethod
            def download_json(url, **kwargs):
                return {
                    "Datasets": {
                        "Dataset": TestFaostat.indicatorsets[
                            "Food Security and Nutrition"
                        ]
                    }
                }

            @staticmethod
            def download_file(url, path=None, **kwargs):
                if url == "https://lala/Food_Security_Data_E_All_Data_(Normalized).zip":
                    shutil.copyfile(join("tests", "fixtures", basename(path)), path)
                    return path
                raise DownloadError("Should not get here!")

            @staticmethod
            def get_tabular_rows(path, **kwargs):
                if path == "mypath":
                    return ["Country Code", "ISO3 Code", "Country"], [
                        {
                            "Country Code": "2",
                            "ISO3 Code": "AFG",
                            "Country": "Afghanistan",
                        }
                    ]
                elif "FS.csv" in str(path):
                    return [
                        "Iso3",
                        "StartDate",
                        "EndDate",
                        "Area Code",
                        "Area",
                        "Item Code",
                        "Item",
                        "Element Code",
                        "Element",
                        "Year Code",
                        "Year",
                        "Unit",
                        "Value",
                        "Flag",
                    ], [
                        {
                            "Area Code": "2",
                            "Area": "Afghanistan",
                            "Item Code": "21010",
                            "Item": "Average dietary energy supply adequacy (percent) (3-year average)",
                            "Element Code": "6121",
                            "Element": "Value",
                            "Year Code": "19992001",
                            "Year": "1999-2001",
                            "Unit": "%",
                            "Value": "89.000000",
                            "Flag": "F",
                        },
                        {
                            "Area Code": "2",
                            "Area": "Afghanistan",
                            "Item Code": "21011",
                            "Item": "Average value of food production (constant 2004-2006 I$/cap) (3-year average)",
                            "Element Code": "6122",
                            "Element": "Value",
                            "Year Code": "20042006",
                            "Year": "2004-2006",
                            "Unit": "I$ per person",
                            "Value": "114.000000",
                            "Flag": "F",
                        },
                        {
                            "Area Code": "2",
                            "Area": "Afghanistan",
                            "Item Code": "22013",
                            "Item": "Gross domestic product per capita, PPP, dissemination (constant 2011 international $)",
                            "Element Code": "6126",
                            "Element": "Value",
                            "Year Code": "2014",
                            "Year": "2014",
                            "Unit": "I$",
                            "Value": "1839.000000",
                            "Flag": "X",
                        },
                        {
                            "Area Code": "3",
                            "Area": "XXX",
                            "Item Code": "1111",
                            "Item": "lala",
                            "Element Code": "111",
                            "Element": "Value",
                            "Year Code": "2014",
                            "Year": "2014",
                            "Unit": "I$",
                            "Value": "1839.000000",
                            "Flag": "X",
                        },
                    ]

        with temp_dir("faostat-retriever") as tmpdir:
            yield Retrieve(
                downloader=MockDownloader(),
                fallback_dir=tmpdir,
                saved_dir=tmpdir,
                temp_dir=tmpdir,
                save=False,
                use_saved=False,
            )

    def test_get_countries(self, retriever):
        countries, countrymapping = get_countries("mypath", retriever)
        assert countries == [TestFaostat.country]
        assert countrymapping == TestFaostat.countrymapping

    def test_codes_filter(self):
        # FBS is allowed; CB shares the same category prefix but is not in codes list
        fsurl = "https://lala/Food_Security_Data_E_All_Data_(Normalized).zip"
        categories = {
            "Food Balances": {
                "title": "Food Balance Sheets",
                "filename": "faostat-food-balance-sheets-for-",
                "codes": {"FBS": "faostat-food-balances"},
            }
        }

        class MockDownloader:
            @staticmethod
            def download_json(url, **kwargs):
                return {
                    "Datasets": {
                        "Dataset": [
                            {
                                "DatasetCode": "FBS",
                                "DatasetName": "Food Balances: Food Balances (2010-)",
                                "DatasetDescription": "Food balance sheets.",
                                "FileLocation": fsurl,
                            },
                            {
                                "DatasetCode": "CB",
                                "DatasetName": "Food Balances: Commodity Balances (non-food) (2010-)",
                                "DatasetDescription": "Commodity balances.",
                                "FileLocation": "https://lala/CommodityBalances_E_All_Data_(Normalized).zip",
                            },
                        ]
                    }
                }

            @staticmethod
            def download_file(url, path=None, **kwargs):
                shutil.copyfile(join("tests", "fixtures", "FS.zip"), path)
                return path

            @staticmethod
            def get_tabular_rows(path, **kwargs):
                return [], []

        with temp_dir("faostat-codes-filter") as tmpdir:
            test_retriever = Retrieve(
                downloader=MockDownloader(),
                fallback_dir=tmpdir,
                saved_dir=tmpdir,
                temp_dir=tmpdir,
                save=False,
                use_saved=False,
            )
            indicatorsets = download_indicatorsets(
                "https://lala/datasets_E.json",
                categories,
                test_retriever,
                tmpdir,
            )
        assert "Food Balances" in indicatorsets
        codes_in_result = [r["DatasetCode"] for r in indicatorsets["Food Balances"]]
        assert "FBS" in codes_in_result
        assert "CB" not in codes_in_result

    def test_generate_dataset_and_showcase(self, configuration, retriever):
        with temp_dir("faostat-test") as folder:
            indicatorsets = download_indicatorsets(
                configuration["filelist_url"],
                configuration["categories"],
                retriever,
                folder,
            )
            assert indicatorsets == TestFaostat.indicatorsets

            filelist_url = configuration["filelist_url"]
            showcase_base_url = configuration["showcase_base_url"]
            (
                dataset,
                showcase,
            ) = generate_dataset_and_showcase(
                "Food Security and Nutrition",
                configuration["categories"],
                TestFaostat.indicatorsets,
                TestFaostat.country,
                TestFaostat.countrymapping,
                showcase_base_url,
                filelist_url,
                retriever,
                folder,
            )
            assert dataset == {
                "name": "afg-faostat-food-security-indicators",
                "title": "Afghanistan - Food Security and Nutrition Indicators",
                "notes": "Food Security and Nutrition Indicators for Afghanistan.\n\nContains data from the FAOSTAT [bulk data service](https://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json).",
                "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                "owner_org": "ed727a5b-3e6e-4cd6-b97e-4a71532085e6",
                "data_update_frequency": "365",
                "subnational": "0",
                "tags": [
                    {
                        "name": "food security",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "nutrition",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
                "groups": [{"name": "afg"}],
                "dataset_date": "[1999-01-01T00:00:00 TO 2014-12-31T23:59:59]",
            }

            resources = dataset.get_resources()
            assert resources == [
                {
                    "name": "Food Security and Nutrition: Suite of Food Security Indicators for Afghanistan",
                    "description": "*Food Security and Nutrition: Suite of Food Security Indicators:*\nFor detailed description of the indicators below see attached document: Average Dietary Supply Adequacy;...",
                    "format": "csv",
                },
            ]
            assert showcase == {
                "name": "afg-faostat-food-security-indicators-showcase",
                "title": "Afghanistan - Food Security and Nutrition Indicators",
                "notes": """Food Security and Nutrition Data Dashboard for Afghanistan\n\n
FAO statistics collates and disseminates food and agricultural
statistics globally. The division develops methodologies and standards
for data collection, and holds regular meetings and workshops to support
member countries develop statistical systems. We produce publications,
working papers and statistical yearbooks that cover food security, prices,
production and trade and agri-environmental statistics.""",
                "url": "https://www.fao.org/faostat/en/#country/2",
                "image_url": "https://www.fao.org/uploads/pics/food-agriculture.png",
                "tags": [
                    {
                        "name": "food security",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "nutrition",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
            }
            file = "afg_faostat_food_security_indicators.csv"
            assert_files_same(join("tests", "fixtures", file), join(folder, file))
