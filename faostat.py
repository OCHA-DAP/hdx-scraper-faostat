#!/usr/bin/python
"""
FAOSTAT:
-------

Reads FAOSTAT JSON and creates datasets.

"""

import logging
from datetime import datetime, timedelta
from os import remove, rename
from os.path import basename, exists, getctime, join
from urllib.parse import urlsplit
from zipfile import ZipFile

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.dictandlist import dict_of_lists_add
from slugify import slugify

logger = logging.getLogger(__name__)

description = "FAO statistics collates and disseminates food and agricultural statistics globally. The division develops methodologies and standards for data collection, and holds regular meetings and workshops to support member countries develop statistical systems. We produce publications, working papers and statistical yearbooks that cover food security, prices, production and trade and agri-environmental statistics."
hxltags = {
    "Iso3": "#country+code",
    "StartDate": "#date+start",
    "EndDate": "#date+end",
    "Year": "#date+year",
    "Area": "#country+name",
    "Item Code": "#indicator+code",
    "Item": "#indicator+name",
    "Unit": "#indicator+type",
    "Value": "#indicator+value+num",
}


def download_indicatorsets(filelist_url, categories, downloader, folder):
    indicatorsets = dict()
    response = downloader.download(filelist_url)
    jsonresponse = response.json()

    def add_row(row, filepath, categoryname, category):
        row["path"] = filepath
        quickcharts = category.get("quickcharts")
        if quickcharts and row["DatasetCode"] == quickcharts["code"]:
            row["quickcharts"] = quickcharts["indicators"]
        else:
            row["quickcharts"] = None
        dict_of_lists_add(indicatorsets, categoryname, row)

    for row in jsonresponse["Datasets"]["Dataset"]:
        for categoryname in categories:
            category = categories[categoryname]
            datasetname = row["DatasetName"]
            if f"{categoryname}:" not in datasetname or "archive" in datasetname.lower():
                continue
            filelocation = row["FileLocation"]
            urlpath = urlsplit(filelocation).path
            filename = basename(urlpath).replace("zip", "csv")
            if "Archive" in filename:
                continue
            indicatorsetcode = row["DatasetCode"]
            filepath = join(folder, f"{indicatorsetcode}.csv")
            statusfile = join(folder, f"{indicatorsetcode}.txt")
            if exists(filepath):
                if exists(statusfile):
                    filedate = datetime.fromtimestamp(getctime(statusfile))
                    if filedate > (datetime.now() - timedelta(days=1)):
                        with open(statusfile) as f:
                            status = f.read()
                            if status == "OK":
                                add_row(row, filepath, categoryname, category)
                                continue
                    remove(statusfile)
                remove(filepath)
            path = filepath.replace(".csv", ".zip")
            if exists(path):
                remove(path)
            path = downloader.download_file(filelocation, path=path)
            with ZipFile(path, "r") as zip:
                path = zip.extract(filename, path=folder)
                rename(path, filepath)
                with open(statusfile, "w") as f:
                    f.write("OK")
                add_row(row, filepath, categoryname, category)
    return indicatorsets


def get_countries(countries_url, downloader):
    countrymapping = dict()

    _, iterator = downloader.get_tabular_rows(
        countries_url, headers=1, dict_form=True, format="csv"
    )
    for row in iterator:
        countryiso = row["ISO3 Code"]
        if not countryiso:
            continue
        countryiso = countryiso.strip()
        if not countryiso:
            continue
        try:
            int(countryiso)
            continue
        except ValueError:
            pass
        countrymapping[row["Country Code"].strip()] = (
            countryiso,
            row["Country"].strip(),
        )
    countries = list()
    for countryiso, countryname in sorted(countrymapping.values()):
        newcountryname = Country.get_country_name_from_iso3(countryiso)
        if newcountryname:
            countries.append(
                {
                    "iso3": countryiso,
                    "countryname": newcountryname,
                    "origname": countryname,
                }
            )
    return countries, countrymapping


def generate_dataset_and_showcase(
    categoryname,
    categories,
    indicatorsets,
    country,
    countrymapping,
    showcase_base_url,
    filelist_url,
    downloader,
    folder,
):
    countryiso = country["iso3"]
    countryname = country["countryname"]
    category = categories[categoryname]
    indicatorset = indicatorsets[categoryname]
    indicatorsetdisplayname = category["title"]
    title = f"{countryname} - {indicatorsetdisplayname}"
    slugified_name = slugify(f"{category['filename']}{countryname.lower()}")
    logger.info(f"Creating dataset: {title}")
    dataset = Dataset({"name": slugified_name, "title": title})
    dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
    dataset.set_organization("ed727a5b-3e6e-4cd6-b97e-4a71532085e6")
    dataset.set_expected_update_frequency("Every year")
    dataset.set_subnational(False)
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception(f"{countryname} has a problem! {e}")
        return None, None, None, None
    tags = ["hxl", "indicators"]
    tag = categoryname.lower()
    if " - " in tag:
        tags.extend(tag.split(" - "))
    elif " and " in tag:
        tags.extend(tag.split(" and "))
    else:
        tags.append(tag)
    dataset.add_tags(tags)

    def process_date(row):
        countrycode = row.get("Area Code")
        if countrycode is None:
            return None
        result = countrymapping.get(countrycode)
        if result is None:
            return None
        isolookup, _ = result
        if isolookup != countryiso:
            return None
        row["Iso3"] = countryiso
        year = row["Year"]
        month = row.get("Months")
        if month is not None and month != "Annual value":
            startdate, enddate = parse_date_range(f"{month} {year}")
        else:
            if "-" in year:
                yearrange = year.split("-")
                startdate, _ = parse_date_range(yearrange[0])
                _, enddate = parse_date_range(yearrange[1])
                row["Year"] = yearrange[1]
            else:
                startdate, enddate = parse_date_range(year)
        row["StartDate"] = startdate.strftime("%Y-%m-%d")
        row["EndDate"] = enddate.strftime("%Y-%m-%d")
        return {"startdate": startdate, "enddate": enddate}

    bites_disabled = [True, True, True]
    qc_indicators = None
    categories = list()
    for row in indicatorset:
        longname = row["DatasetName"]
        url = row["path"]
        category = longname.split(": ")[1]
        filename = f"{category}_{countryiso}.csv"
        description = f"*{category}:*\n{row['DatasetDescription']}"
        if category[-10:] == "Indicators":
            name = category
        else:
            name = f"{category} data"
        resourcedata = {"name": f"{name} for {countryname}", "description": description}
        header_insertions = [(0, "EndDate"), (0, "StartDate"), (0, "Iso3")]
        indicators_for_qc = row.get("quickcharts")
        if indicators_for_qc:
            quickcharts = {
                "hashtag": "#indicator+code",
                "values": [x["code"] for x in indicators_for_qc],
                "numeric_hashtag": "#indicator+value+num",
                "cutdown": 2,
                "cutdownhashtags": ["#indicator+code", "#country+code", "#date+year"],
            }
            qc_indicators = indicators_for_qc
        else:
            quickcharts = None
        success, results = dataset.download_and_generate_resource(
            downloader,
            url,
            hxltags,
            folder,
            filename,
            resourcedata,
            header_insertions=header_insertions,
            date_function=process_date,
            quickcharts=quickcharts,
            encoding="WINDOWS-1252",
        )
        if success is False:
            logger.warning(f"{category} for {countryname} has no data!")
            continue
        disabled_bites = results.get("bites_disabled")
        if disabled_bites:
            bites_disabled = disabled_bites
        categories.append(category)

    if dataset.number_of_resources() == 0:
        logger.warning(f"{countryname} has no data!")
        return None, None, None, None
    dataset.quickcharts_resource_last()
    notes = [
        f"{indicatorsetdisplayname} for {countryname}.\n\n",
        f"Contains data from the FAOSTAT [bulk data service]({filelist_url})",
    ]
    if len(categories) == 1:
        notes.append(".")
    else:
        notes.append(f" covering the following categories: {', '.join(categories)}")
    dataset["notes"] = "".join(notes)

    showcase = Showcase(
        {
            "name": f"{slugified_name}-showcase",
            "title": title,
            "notes": f"{categoryname} Data Dashboard for {countryname}",
            "url": f"{showcase_base_url}{countryiso}",
            "image_url": "https://pbs.twimg.com/profile_images/1375385494167691269/Bc49-Yx8_400x400.jpg",
        }
    )
    showcase.add_tags(tags)
    return dataset, showcase, bites_disabled, qc_indicators
