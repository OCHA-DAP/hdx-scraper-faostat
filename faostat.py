#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
FAOSTAT:
-------

Reads FAOSTAT JSON and creates datasets.

"""

import logging
from copy import deepcopy
from os.path import join
from tempfile import gettempdir

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import write_list_to_csv
from slugify import slugify

logger = logging.getLogger(__name__)

hxltags = {'Iso3': '#country+code+v_iso3', 'Area': '#country+name', 'Item Code': '#indicator+code',
           'Item': '#indicator+name', 'StartYear': '#date+start', 'EndYear': '#date+end', 'Unit': '#indicator+type',
           'Value': '#indicator+num'}


def get_countriesdata(countries_url, downloader):
    countries = dict()

    for row in downloader.get_tabular_rows(countries_url, dict_rows=True, headers=1, format='csv'):
        countries[row['Country Code']] = (row['ISO3 Code'], row['Country'].strip())
    return countries


def get_indicatortypesdata(filelist_url, downloader):
    response = downloader.download(filelist_url)
    jsonresponse = response.json()
    indicatortypeslist = jsonresponse['Datasets']['Dataset']
    return {x['DatasetCode']: x for x in indicatortypeslist}


def generate_datasets_and_showcases(downloader, indicatorname, indicatortypedata, countriesdata, showcase_base_url):
    dataset_template = Dataset()
    dataset_template.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset_template.set_organization('ed727a5b-3e6e-4cd6-b97e-4a71532085e6')
    dataset_template.set_expected_update_frequency('Every year')
    dataset_template.set_subnational(False)
    tags = [indicatorname.lower()]
    dataset_template.add_tags(tags)

    tmpdir = gettempdir()
    earliest_year = 10000
    latest_year = 0
    countrycode = None
    iso3 = None
    countryname = None
    rows = None
    datasets = list()
    showcases = list()
    for row in downloader.get_tabular_rows(indicatortypedata['FileLocation'], dict_rows=True, headers=1, format='csv',
                                           encoding='ISO 8859-1'):
        newcountry = row['Area Code']
        if newcountry != countrycode:
            if countrycode is not None:
                filepath = join(tmpdir, '%s_%s.csv' % (indicatorname, countrycode))
                headers = deepcopy(downloader.response.headers)
                for i, header in enumerate(headers):
                    if 'year' in header.lower():
                        headers.insert(i, 'EndYear')
                        headers.insert(i, 'StartYear')
                        break
                headers.insert(0, 'Iso3')
                hxlrow = dict()
                for header in headers:
                    hxlrow[header] = hxltags.get(header, '')
                rows.insert(0, hxlrow)
                write_list_to_csv(rows, filepath, headers=headers)
                ds = datasets[-1]
                ds.set_dataset_year_range(earliest_year, latest_year)
                ds.resources[0].set_file_to_upload(filepath)

            rows = list()
            countrycode = newcountry
            dataset = Dataset(deepcopy(dataset_template.data))
            result = countriesdata.get(countrycode)
            if result is None:
                logger.warning('Ignoring %s' % countrycode)
                continue
            iso3, cn = result
            countryname = Country.get_country_name_from_iso3(iso3)
            if countryname is None:
                logger.error('Missing country %s: %s, %s' % (countrycode, cn, iso3))
                continue
            title = '%s - %s Indicators' % (countryname, indicatorname)
            logger.info('Generating dataset: %s' % title)
            name = 'FAOSTAT %s indicators for %s' % (countryname, indicatorname)
            slugified_name = slugify(name).lower()
            dataset['name'] = slugified_name
            dataset['title'] = title
            dataset.update_from_yaml()
            notes = dataset['notes']
            dataset['notes'] = '%s\n\n\n%s' % (notes, indicatortypedata['DatasetDescription'])
            dataset.add_country_location(countryname)
            earliest_year = 10000
            latest_year = 0

            resource = Resource({
                'name': title,
                'description': ''
            })
            resource.set_file_type('csv')
            dataset.add_update_resource(resource)
            datasets.append(dataset)
            showcase = Showcase({
                'name': '%s-showcase' % slugified_name,
                'title': title,
                'notes': notes,
                'url': '%s%s' % (showcase_base_url, countrycode),
                'image_url': 'http://www.fao.org/uploads/pics/food-agriculture.png'
            })
            showcase.add_tags(tags)
            showcases.append(showcase)
        row['Iso3'] = iso3
        row['Area'] = countryname
        year = row['Year']
        if '-' in year:
            years = year.split('-')
            row['StartYear'] = years[0]
            row['EndYear'] = years[1]
        else:
            years = [year]
            row['StartYear'] = year
            row['EndYear'] = year
        for year in years:
            year = int(year)
            if year < earliest_year:
                earliest_year = year
            if year > latest_year:
                latest_year = year
        rows.append(row)

    return datasets, showcases
