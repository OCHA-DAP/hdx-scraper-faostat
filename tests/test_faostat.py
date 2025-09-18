#!/usr/bin/python
"""
Unit tests for FAOSTAT.

"""

import shutil
from os.path import basename, join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import DownloadError
from hdx.utilities.path import temp_dir

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
            project_config_yaml=join("tests", "config", "project_configuration.yaml"),
        )
        Locations.set_validlocations(
            [{"name": "afg", "title": "Afghanistan"}]
        )  # add locations used in tests
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = {}
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": "hxl"},
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
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            response = Response()
            response.headers = [
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
            ]

            @staticmethod
            def hxl_row(headers, hxltags, dict_form):
                return {header: hxltags.get(header, "") for header in headers}

            @staticmethod
            def download(url):
                response = Response()
                if url == "https://lala/datasets_E.json":

                    def fn():
                        return {
                            "Datasets": {
                                "Dataset": TestFaostat.indicatorsets[
                                    "Food Security and Nutrition"
                                ]
                            }
                        }

                    response.json = fn
                return response

            @staticmethod
            def download_file(url, path):
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
                elif "FS.csv" in path:
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

        return Download()

    def test_download_indicatorsets(self, configuration, downloader, mock_urlretrieve):
        with temp_dir("faostat-test") as folder:
            indicatorsets = download_indicatorsets(
                configuration["filelist_url"],
                configuration["categories"],
                downloader,
                folder,
            )
            assert indicatorsets == TestFaostat.indicatorsets

    def test_get_countries(self, downloader):
        countries, countrymapping = get_countries("mypath", downloader)
        assert countries == [TestFaostat.country]
        assert countrymapping == TestFaostat.countrymapping

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        with temp_dir("faostat-test") as folder:
            filelist_url = configuration["filelist_url"]
            showcase_base_url = configuration["showcase_base_url"]
            (
                dataset,
                showcase,
                bites_disabled,
                qc_indicators,
            ) = generate_dataset_and_showcase(
                "Food Security and Nutrition",
                configuration["categories"],
                TestFaostat.indicatorsets,
                TestFaostat.country,
                TestFaostat.countrymapping,
                showcase_base_url,
                filelist_url,
                downloader,
                folder,
            )
            assert dataset == {
                "name": "faostat-food-security-indicators-for-afghanistan",
                "title": "Afghanistan - Food Security and Nutrition Indicators",
                "notes": "Food Security and Nutrition Indicators for Afghanistan.\n\nContains data from the FAOSTAT [bulk data service](https://lala/datasets_E.json).",
                "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                "owner_org": "ed727a5b-3e6e-4cd6-b97e-4a71532085e6",
                "data_update_frequency": "365",
                "subnational": "0",
                "tags": [
                    {
                        "name": "hxl",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "indicators",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
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
                    "name": "Suite of Food Security Indicators for Afghanistan",
                    "description": "*Suite of Food Security Indicators:*\nFor detailed description of the indicators below see attached document: Average Dietary Supply Adequacy;...",
                    "format": "csv",
                },
                {
                    "name": "QuickCharts-Suite of Food Security Indicators for Afghanistan",
                    "description": "Cut down data for QuickCharts",
                    "format": "csv",
                },
            ]
            assert showcase == {
                "name": "faostat-food-security-indicators-for-afghanistan-showcase",
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
                        "name": "hxl",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "indicators",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
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
            assert bites_disabled == [False, True, True]
            assert qc_indicators == [
                {
                    "code": "21010",
                    "title": "Average dietary energy supply adequacy",
                    "unit": "Percentage",
                },
                {
                    "code": "210041",
                    "title": "Prevalence of undernourishment",
                    "unit": "Percentage",
                },
                {
                    "code": "21034",
                    "title": "Percentage of arable land equipped for irrigation",
                    "unit": "Percentage",
                },
            ]
            file = "Suite of Food Security Indicators_AFG.csv"
            assert_files_same(join("tests", "fixtures", file), join(folder, file))
            file = f"qc_{file}"
            assert_files_same(join("tests", "fixtures", file), join(folder, file))
