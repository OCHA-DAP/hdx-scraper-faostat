### Collector for FAOSTAT Datasets
[![Run tests](https://github.com/OCHA-DAP/hdx-scraper-faostat/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-faostat/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-faostat/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-faostat?branch=main)

FAOSTAT script scrapes food security data from [FAOSTAT](http://www.fao.org/faostat/en/) website and extracts population data country by country creating a dataset per country in HDX. It makes a three reads from FAOStat and then 1000 read/writes (API calls) to HDX in a one hour period. It creates 200 temporary files each around 50kb. It runs every week. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-faostat** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, BASIC_AUTH, EXTRA_PARAMS, TEMP_DIR, LOG_FILE_ONLY