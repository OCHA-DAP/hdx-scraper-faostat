### Collector for FAOSTAT Datasets
[![Build Status](https://travis-ci.org/OCHA-DAP/hdxscraper-faostat.svg?branch=master&ts=1)](https://travis-ci.org/OCHA-DAP/hdxscraper-faostat) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdxscraper-faostat/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdxscraper-faostat?branch=master)

FAOSTAT script scrapes food security data from [FAOSTAT](http://www.fao.org/faostat/en/) website and uploads disaggregated by country to HDX.

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
