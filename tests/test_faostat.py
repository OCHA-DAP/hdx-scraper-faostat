#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for FAOSTAT.

'''
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.utilities.path import temp_dir

from faostat import generate_datasets_and_showcases, get_countriesdata, get_indicatortypesdata


class TestFaostat:
    countrydata = {'2': ('AFG', 'Afghanistan')}
    fsurl = 'http://lala/Food_Security_Data.zip'
    indicatortypedata = {'DatasetCode': 'FS', 'DatasetName': 'Food Security: Suite of Food Security Indicators',
                         'Topic': 'See attached document which lists sector coverage with the respective indicator.',
                         'DatasetDescription': 'For detailed description of the indicators below see attached document: Average Dietary Supply Adequacy; Average Value of Food Production; Share of Dietary Energy Supply Derived from Cereals, Roots and Tubers; Average Protein Supply; Average Supply of Protein of Animal Origin; Percent of paved roads over total roads; Road Density (per 100 square km of land area); Rail lines Density (per 100 square km of land area); Domestic Food Price Level Index; Percentage of Population with Access to Improved Drinking Water Sources; Percentage of Population with Access to Sanitation Facilities; Cereal Import Dependency Ratio; Percent of Arable Land Equipped for Irrigation; Value of Food Imports in Total Merchandise Exports; Political stability and absence of violence; Domestic Food Price Volatility Index; Per capita food production variability; Per capita food supply variability; Prevalence of Undernourishment; Share of Food Expenditures of the Poor; Depth of the Food Deficit; Prevalence of Food Inadequacy; Children aged <5 years wasted (%); Children aged <5 years stunted (%); Children aged <5 years underweight (%); Percentage of adults underweight in total adult population; Prevalence of anaemia among children under 5 years of age; Prevalence of Vitamin A deficiency in the population; Prevalence of Iodine deficiency; Prevalence of anaemia among pregnant women; Number of people undernourished; Minimum Dietary Energy Requirement (MDER); Average Dietary Energy Requirement (ADER); "Minimum Dietary Energy Requirement (MDER) -  PAL 1.75"; Coefficient of variation of habitual caloric consumption distribution (CV); Skewness of habitual caloric consumption distribution (SK); Incidence of caloric losses at retail distribution level; Dietary Energy Supply (DES); Average Fat Supply',
                         'Contact': 'Carlo Cafiero', 'Email': 'Food-Security-Statistics@FAO.org',
                         'DateUpdate': '2018-10-16', 'CompressionFormat': 'zip', 'FileType': 'csv', 'FileSize': '681KB',
                         'FileRows': 70890, 'FileLocation': fsurl}

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}])  # add locations used in tests

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            response = Response()
            response.headers = ['Area Code', 'Area', 'Item Code', 'Item', 'Element Code', 'Element', 'Year Code',
                                'Year',
                                'Unit', 'Value', 'Flag']

            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://xxx/':
                    def fn():
                        return {'Datasets': {'Dataset': [TestFaostat.indicatortypedata]}}
                    response.json = fn
                return response

            @staticmethod
            def get_tabular_rows(url, **kwargs):
                if url == 'http://yyy/':
                    return [{'Country Code': '2', 'ISO3 Code': 'AFG', 'Country': 'Afghanistan'}]
                elif url == TestFaostat.fsurl:
                    return [{'Area Code': '2', 'Area': 'Afghanistan', 'Item Code': '21010',
                             'Item': 'Average dietary energy supply adequacy (percent) (3-year average)',
                             'Element Code': '6121', 'Element': 'Value', 'Year Code': '19992001', 'Year': '1999-2001',
                             'Unit': '%', 'Value': '89.000000', 'Flag': 'F'},
                            {'Area Code': '2', 'Area': 'Afghanistan', 'Item Code': '21011',
                             'Item': 'Average value of food production (constant 2004-2006 I$/cap) (3-year average)',
                             'Element Code': '6122', 'Element': 'Value', 'Year Code': '20042006', 'Year': '2004-2006',
                             'Unit': 'I$ per person', 'Value': '114.000000', 'Flag': 'F'},
                            {'Area Code': '2', 'Area': 'Afghanistan', 'Item Code': '22013',
                             'Item': 'Gross domestic product per capita, PPP, dissemination (constant 2011 international $)',
                             'Element Code': '6126', 'Element': 'Value', 'Year Code': '2014', 'Year': '2014',
                             'Unit': 'I$', 'Value': '1839.000000', 'Flag': 'X'},
                            {'Area Code': '3', 'Area': 'XXX', 'Item Code': '1111', 'Item': 'lala',
                             'Element Code': '111', 'Element': 'Value', 'Year Code': '2014', 'Year': '2014',
                             'Unit': 'I$', 'Value': '1839.000000', 'Flag': 'X'}
                            ]

        return Download()

    def test_get_indicatortypesdata(self, downloader):
        indicatortypesdata = get_indicatortypesdata('http://xxx/', downloader)
        assert indicatortypesdata == {'FS': TestFaostat.indicatortypedata}

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countriesdata('http://yyy/', downloader)
        assert countriesdata == {'2': ('AFG', 'Afghanistan')}

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        with temp_dir('faostat') as folder:
            datasets, showcases = generate_datasets_and_showcases(downloader, folder, 'Food Security',
                                                                  TestFaostat.indicatortypedata,
                                                                  TestFaostat.countrydata, 'http://zzz/')
            assert datasets[0] == {'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                                   'owner_org': 'ed727a5b-3e6e-4cd6-b97e-4a71532085e6', 'data_update_frequency': '365',
                                   'subnational': '0', 'tags': [{'name': 'hxl'}, {'name': 'food security'}],
                                   'name': 'faostat-afghanistan-indicators-for-food-security',
                                   'title': 'Afghanistan - Food Security Indicators', 'license_id': 'cc-by-igo',
                                   'notes': 'FAO statistics collates and disseminates food and agricultural statistics globally. The division develops methodologies and standards for data collection, and holds regular meetings and workshops to support member countries develop statistical systems. We produce publications, working papers and statistical yearbooks that cover food security, prices, production and trade and agri-environmental statistics.',
                                   'caveats': 'Reliability and accuracy depend on the sampling design and size of the basic variables and these might differ significantly between countries just as the use of data sources, definitions and methods. The accuracy of an indicator is very much dependent on the accuracy of the basic variables that make up the indicator.',
                                   'methodology': 'Registry', 'dataset_source': 'FAOSTAT', 'package_creator': 'mcarans',
                                   'private': False, 'groups': [{'name': 'afg'}], 'dataset_date': '01/01/1999-12/31/2014'}

            resources = datasets[0].get_resources()
            assert resources == [{'name': 'Afghanistan - Food Security Indicators', 'description': '', 'format': 'csv'}]
            assert showcases[0] == {'name': 'faostat-afghanistan-indicators-for-food-security-showcase',
                                    'title': 'Afghanistan - Food Security Indicators',
                                    'notes': 'FAO statistics collates and disseminates food and agricultural statistics globally. The division develops methodologies and standards for data collection, and holds regular meetings and workshops to support member countries develop statistical systems. We produce publications, working papers and statistical yearbooks that cover food security, prices, production and trade and agri-environmental statistics.',
                                    'url': 'http://zzz/2',
                                    'image_url': 'http://www.fao.org/uploads/pics/food-agriculture.png',
                                    'tags': [{'name': 'hxl'}, {'name': 'food security'}]}
