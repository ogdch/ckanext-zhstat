ckanext-zhstat
===========

Harvester for the Statistical Office of Canton of Zurich

## Installation

Use `pip` to install this plugin. This example installs it in `/home/www-data`

```bash
source /home/www-data/pyenv/bin/activate
pip install -e git+https://github.com/ogdch/ckanext-zhstat.git#egg=ckanext-zhstat --src /home/www-data
cd /home/www-data/ckanext-zhstat
pip install -r pip-requirements.txt
python setup.py develop
```

Make sure to add `zhstat` and `zhstat_harvester` to `ckan.plugins` in your config file.

## Run harvester

```bash
source /home/www-data/pyenv/bin/activate
paster --plugin=ckanext-zhstat zhstat_harvester gather_consumer -c development.ini &
paster --plugin=ckanext-zhstat zhstat_harvester fetch_consumer -c development.ini &
paster --plugin=ckanext-zhstat zhstat_harvester run -c development.ini
```
