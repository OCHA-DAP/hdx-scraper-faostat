### Template Usage

Replace scrapername everywhere with your scraper's name eg. worldbank
Replace ScraperName everywhere with your scraper's name eg. World Bank
Look for xxx and ... and replace add text accordingly.

Scrapers can be installed on QuickCode and set up to run on a schedule 
using the command in the file "crontab".

Collector designed to collect ScraperName datasets from the [ScraperName](http://) website and to automatically register datasets on the [Humanitarian Data Exchange](http://data.humdata.org/) project.

For full scrapers following this template see:
[ACLED](https://github.com/OCHA-DAP/hdxscraper-acled-africa),
[FTS](https://github.com/OCHA-DAP/hdxscraper-fts),
[WHO](https://github.com/OCHA-DAP/hdxscraper-who),
[World Bank](https://github.com/OCHA-DAP/hdxscraper-worldbank),
[WorldPop](https://github.com/OCHA-DAP/hdxscraper-worldpop)

For a scraper that also creates datasets disaggregated by indicator (not just country) and
reads metadata from a Google spreadsheet exported as csv, see:
[IDMC](https://github.com/OCHA-DAP/hdxscraper-idmc)

### Collector for ScraperName's Datasets
[![Build Status](https://travis-ci.org/OCHA-DAP/hdxscraper-scrapername.svg?branch=master&ts=1)](https://travis-ci.org/OCHA-DAP/hdxscraper-scrapername) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdxscraper-scrapername/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdxscraper-scrapername?branch=master)

### Usage
python run.py

For the script to run, you will need to either pass in your HDX API key as a parameter or have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: test
    
 You will also need to pass in your user agent as a parameter or pass a parameter *user_agent_config_yaml* specifying where your user agent file is located. It should be of the form:
 
    user_agent: MY_USER_AGENT
    
 If you have many user agents, you can create a file of this form, put its location in *user_agent_config_yaml* and specify the lookup in *user_agent_lookup*:
 
    myscraper:
        user_agent: MY_USER_AGENT
    myscraper2:
        user_agent: MY_USER_AGENT2

 Note for HDX scrapers: there is a universal .useragents.yml file you should use.
