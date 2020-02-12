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

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import write_list_to_csv
from slugify import slugify

logger = logging.getLogger(__name__)

hxltags = {'Iso3': '#country+code+v_iso3', 'Area': '#country+name', 'Item Code': '#indicator+code',
           'Item': '#indicator+name', 'StartYear': '#date+year+start', 'EndYear': '#date+year+end',
           'Unit': '#indicator+type',
           'Value': '#indicator+num'}


def get_countries(countries_url, downloader):
    countries = dict()

    _, iterator = downloader.get_tabular_rows(countries_url, headers=1, dict_rows=True, format='csv')
    for row in iterator:
        countries[row['Country Code'].strip()] = (row['ISO3 Code'].strip(), row['Country'].strip())
    return countries


def get_indicatortypes(filelist_url, downloader):
    response = downloader.download(filelist_url)
    jsonresponse = response.json()
    indicatortypeslist = jsonresponse['Datasets']['Dataset']
    return {x['DatasetCode']: x for x in indicatortypeslist}


def generate_datasets_and_showcases(downloader, folder, indicatorname, indicatortypedata,
                                    countriesdata, showcase_base_url):
    dataset_template = Dataset()
    dataset_template.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset_template.set_organization('ed727a5b-3e6e-4cd6-b97e-4a71532085e6')
    dataset_template.set_expected_update_frequency('Every year')
    dataset_template.set_subnational(False)
    tags = ['hxl', 'indicators', indicatorname.lower()]
    dataset_template.add_tags(tags)

    years = set()
    countrycode = None
    iso3 = None
    countryname = None
    rows = None
    datasets = list()
    showcases = list()

    def output_csv(cname, iname):
        if rows is None:
            return
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
        filepath = join(folder, '%s_%s.csv' % (iname, countrycode))
        write_list_to_csv(filepath, rows, headers=headers)
        ds = datasets[-1]
        sorted_years = sorted(list(years))
        ds.set_dataset_year_range(sorted_years[0], sorted_years[-1])
        rs = Resource({
            'name': '%s - %s' % (cname, iname),
            'description': 'HXLated csv containing %s indicators for %s' % (iname.lower(), cname)
        })
        rs.set_file_to_upload(filepath)
        rs.set_file_type('csv')
        ds.add_update_resource(rs)

    headers, iterator = downloader.get_tabular_rows(indicatortypedata['FileLocation'], headers=1, dict_rows=True,
                                                    format='csv', encoding='WINDOWS-1252')
    for row in iterator:
        newcountry = row['Area Code']
        if newcountry != countrycode:
            output_csv(countryname, indicatorname)
            rows = None
            countrycode = newcountry
            result = countriesdata.get(countrycode)
            if result is None:
                logger.warning('Ignoring %s' % countrycode)
                continue
            iso3, cn = result
            countryname = Country.get_country_name_from_iso3(iso3)
            if countryname is None:
                logger.error('Missing country %s: %s, %s' % (countrycode, cn, iso3))
                continue
            rows = list()
            title = '%s - %s Indicators' % (countryname, indicatorname)
            logger.info('Generating dataset: %s' % title)
            name = 'FAOSTAT %s indicators for %s' % (countryname, indicatorname)
            slugified_name = slugify(name).lower()
            dataset = Dataset(deepcopy(dataset_template.data))
            dataset['name'] = slugified_name
            dataset['title'] = title
            dataset.update_from_yaml()
            dataset.add_country_location(countryname)
            years.clear()

            datasets.append(dataset)
            showcase = Showcase({
                'name': '%s-showcase' % slugified_name,
                'title': title,
                'notes': dataset['notes'],
                'url': '%s%s' % (showcase_base_url, countrycode),
                'image_url': 'http://www.fao.org/uploads/pics/food-agriculture.png'
            })
            showcase.add_tags(tags)
            showcases.append(showcase)
        row['Iso3'] = iso3
        row['Area'] = countryname
        year = row['Year']
        if '-' in year:
            yearrange = year.split('-')
            row['StartYear'] = yearrange[0]
            row['EndYear'] = yearrange[1]
            years.add(int(yearrange[0]))
            years.add(int(yearrange[1]))
        else:
            years.add(int(year))
            row['StartYear'] = year
            row['EndYear'] = year
        if rows is not None:
            rows.append(row)
    output_csv(countryname, indicatorname)
    return datasets, showcases
