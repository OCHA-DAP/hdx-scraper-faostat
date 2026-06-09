#!/usr/bin/python
"""
FAOSTAT:
-------

Reads FAOSTAT JSON and creates datasets.

"""

import csv
import logging
from datetime import datetime
from os import rename
from os.path import basename, join
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


def download_indicatorsets(filelist_url, categories, retriever, folder):
    indicatorsets = {}
    jsonresponse = retriever.download_json(filelist_url, "datasets_E.json")

    code_to_category = {}
    for categoryname, category in categories.items():
        for code in category.get("codes", {}):
            code_to_category[code] = categoryname

    def add_row(row, filepath, categoryname):
        row["path"] = filepath
        dict_of_lists_add(indicatorsets, categoryname, row)

    for row in jsonresponse["Datasets"]["Dataset"]:
        datasetname = row["DatasetName"]
        if "archive" in datasetname.lower():
            continue
        indicatorsetcode = row["DatasetCode"]
        categoryname = code_to_category.get(indicatorsetcode)
        if categoryname is None:
            continue
        filelocation = row["FileLocation"]
        urlpath = urlsplit(filelocation).path
        filename = basename(urlpath).replace("zip", "csv")
        if "Archive" in filename:
            continue
        filepath = join(folder, f"{indicatorsetcode}.csv")
        zip_path = retriever.download_file(
            filelocation, filename=f"{indicatorsetcode}.zip"
        )
        with ZipFile(zip_path, "r") as z:
            extracted = z.extract(filename, path=folder)
            rename(extracted, filepath)
        add_row(row, filepath, categoryname)
    return indicatorsets


def get_countries(countries_path, retriever):
    countrydata = set()
    countrymapping = {}

    _, iterator = retriever.downloader.get_tabular_rows(
        countries_path, headers=1, dict_form=True, format="csv"
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
        countrycode = row["Country Code"].strip()
        countryname = row["Country"].strip()
        countrydata.add((countryiso, countryname, countrycode))
        countrymapping[row["Country Code"].strip()] = (
            countryiso,
            row["Country"].strip(),
        )
    countries = []
    for countryiso, countryname, countrycode in sorted(countrydata):
        if Country.get_gho_status_from_iso3(
            countryiso
        ) or Country.get_hrp_status_from_iso3(countryiso):
            newcountryname = Country.get_country_name_from_iso3(countryiso)
            if newcountryname:
                countries.append(
                    {
                        "iso3": countryiso,
                        "countryname": newcountryname,
                        "origname": countryname,
                        "countrycode": countrycode,
                    }
                )
    return countries, countrymapping


def log_latest_dates(indicatorsets, countrycodes):
    seen = {}
    for indicatorset in indicatorsets.values():
        for row in indicatorset:
            code = row["DatasetCode"]
            if code not in seen:
                seen[code] = row["path"]
    for code, filepath in sorted(seen.items()):
        max_year = None
        max_month = None
        with open(filepath, encoding="WINDOWS-1252") as f:
            for data_row in csv.DictReader(f):
                countrycode = data_row.get("Area Code")
                if countrycode is None:
                    continue
                if countrycode not in countrycodes:
                    continue
                year = data_row.get("Year")
                if not year:
                    continue
                try:
                    end_year = int(year.split("-")[-1].strip())
                except ValueError:
                    continue
                month_str = data_row.get("Months")
                month = None
                if month_str and month_str != "Annual value":
                    try:
                        month = datetime.strptime(month_str, "%B").month
                    except ValueError:
                        pass
                if max_year is None or end_year > max_year:
                    max_year = end_year
                    max_month = month
                elif end_year == max_year:
                    if month is not None and (max_month is None or month > max_month):
                        max_month = month
        if max_year is not None:
            if max_month is not None:
                label = datetime(max_year, max_month, 1).strftime("%B %Y")
            else:
                label = str(max_year)
            logger.info(f"Latest date for {code}: {label}")


def generate_dataset_and_showcase(
    categoryname,
    categories,
    indicatorsets,
    country,
    countrymapping,
    showcase_base_url,
    filelist_url,
    retriever,
    folder,
):
    countryiso = country["iso3"]
    countryname = country["countryname"]
    countrycode = country["countrycode"]
    category = categories[categoryname]
    indicatorset = indicatorsets[categoryname]
    indicatorsetdisplayname = category["title"]
    title = f"{countryname} - {indicatorsetdisplayname}"
    slugified_name = slugify(f"{countryiso.lower()}-{category['filename']}")
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
        return None, None
    tags = category.get("tags", [])
    dataset.add_tags(tags)
    codes_config = category.get("codes", {})

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

    categories = []
    for row in indicatorset:
        longname = row["DatasetName"]
        url = row["path"]
        category = longname
        indicatorsetcode = row["DatasetCode"]
        description_part = (
            codes_config[indicatorsetcode].removeprefix("faostat-").replace("-", "_")
        )
        filename = f"{countryiso.lower()}_faostat_{description_part}.csv"
        description = f"*{category}:*\n{row['DatasetDescription']}"
        if category[-10:] == "Indicators":
            name = category
        else:
            name = f"{category} data"
        resourcedata = {"name": f"{name} for {countryname}", "description": description}
        header_insertions = [(0, "EndDate"), (0, "StartDate"), (0, "Iso3")]
        headers, iterator = retriever.downloader.get_tabular_rows(
            url,
            dict_form=True,
            header_insertions=header_insertions,
            format="csv",
            encoding="WINDOWS-1252",
        )
        success, results = dataset.generate_resource(
            folder,
            filename,
            iterator,
            resourcedata,
            headers,
            date_function=process_date,
        )
        if success is False:
            logger.warning(f"{category} for {countryname} has no data!")
            continue
        categories.append(category)

    if dataset.number_of_resources() == 0:
        logger.warning(f"{countryname} has no data!")
        return None, None
    notes = [
        f"{indicatorsetdisplayname} for {countryname}.\n\n",
        f"Contains data from the FAOSTAT [bulk data service]({filelist_url})",
    ]
    if len(categories) == 1:
        notes.append(".")
    else:
        notes.append(f" covering the following categories: {', '.join(categories)}")
    dataset["notes"] = "".join(notes)

    notes = f"""{categoryname} Data Dashboard for {countryname}\n\n
FAO statistics collates and disseminates food and agricultural
statistics globally. The division develops methodologies and standards
for data collection, and holds regular meetings and workshops to support
member countries develop statistical systems. We produce publications,
working papers and statistical yearbooks that cover food security, prices,
production and trade and agri-environmental statistics."""
    showcase = Showcase(
        {
            "name": f"{slugified_name}-showcase",
            "title": title,
            "notes": notes,
            "url": f"{showcase_base_url}{countrycode}",
            "image_url": "https://www.fao.org/uploads/pics/food-agriculture.png",
        }
    )
    showcase.add_tags(tags)
    return dataset, showcase
