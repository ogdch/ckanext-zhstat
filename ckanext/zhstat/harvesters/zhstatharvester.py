#coding: utf-8

import os
from lxml import etree
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import tempfile

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action, action
from ckan.lib.helpers import json
from ckanext.harvest.harvesters.base import munge_tag

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
from ckanext.harvest.harvesters import HarvesterBase

from pylons import config

import logging
log = logging.getLogger(__name__)

class ZhstatHarvester(HarvesterBase):
    '''
    The harvester for the Statistical Office of Canton of Zurich
    '''

    BUCKET_NAME = 'bar-opendata-ch'
    DATA_PATH = 'Kanton-ZH/Statistik/'
    METADATA_FILE_NAME = 'metadata.xml'

    # Define the keys in the CKAN .ini file
    AWS_ACCESS_KEY = config.get('ckanext.zhstat.access_key')
    AWS_SECRET_KEY = config.get('ckanext.zhstat.secret_key')

    ORGANIZATION = {
        u'de': u'Statistisches Amt des Kantons Zürich',
        u'fr': u'fr_Statistisches Amt des Kantons Zürich',
        u'it': u'it_Statistisches Amt des Kantons Zürich',
        u'en': u'Statistical Office of Canton of Zurich',
    }
    LANG_CODES = ['de', 'fr', 'it', 'en']

    config = {
        'user': u'harvest'
    }

    bucket = None

    def _get_s3_bucket(self):
        '''
        Create an S3 connection to the department bucket
        '''
        if self.bucket is None:
            conn = S3Connection(self.AWS_ACCESS_KEY, self.AWS_SECRET_KEY)
            self.bucket = conn.get_bucket(self.BUCKET_NAME)
        return self.bucket


    def _fetch_metadata(self):
        '''
        Fetching the metadata file for for the Statistical Office of Canton of Zurich from the S3 Bucket and save on disk
        '''
        temp_dir = tempfile.mkdtemp()
        try:
            metadata_file = Key(self._get_s3_bucket())
            metadata_file.key = self.DATA_PATH + self.METADATA_FILE_NAME
            metadata_file_path = os.path.join(temp_dir, self.METADATA_FILE_NAME)
            log.debug('Saving metadata file to %s' % metadata_file_path)
            metadata_file.get_contents_to_filename(metadata_file_path)
            return open(metadata_file_path).read()
        except Exception, e:
            log.exception(e)
            raise

    def _file_is_available(self, file_name):
        '''
        Returns true if the file exists, false otherwise. (logs falses)
        '''
        k = Key(self._get_s3_bucket())
        k.key = self.DATA_PATH + file_name
        if k.exists():
            return True
        else:
            log.debug(str(status) + ': ' + file_name)
            return False


    def _generate_term_translations(self, base_data, dataset):
        '''
        Return all the term_translations for a given data
        '''
        translations = []

        for data in dataset:
            if base_data.get('id') != data.get('id'):
                lang = data.get('{http://www.w3.org/XML/1998/namespace}lang')
                keys = ['title', 'author', 'maintainer']
                for key in keys:
                    if base_data.find(key).text and data.find(key).text:
                        translations.append({
                            'lang_code': lang,
                            'term': base_data.find(key).text,
                            'term_translation': data.find(key).text
                            })

                base_notes_translation = self._generate_notes(base_data)
                other_notes_translation = self._generate_notes(data)
                translations.append({
                    'lang_code': lang,
                    'term': base_notes_translation,
                    'term_translation': other_notes_translation
                    })

        return translations


    def _generate_resources(self, dataset):
        '''
        Return all resources for a given dataset that are available
        '''
        resources = []
        for data in dataset:
            if self._file_is_available(data.find('resource').find('name').text):
                resources.append({
                    'name': data.find('resource').find('name').text,
                    'type': data.find('resource').find('type').text,
                    })
        return resources


    def _generate_metadata(self, base_data, dataset):
        '''
        Return all the necessary metadata to be able to create a data
        '''
        resources = self._generate_resources(dataset)
        translations = self._generate_term_translations(base_data, dataset)

        if len(resources) != 0:
            return {
                'datasetID': base_data.get('id'),
                'title': base_data.find('title').text,
                'notes': self._generate_notes(base_data),
                'author': base_data.find('author').text,
                'maintainer': base_data.find('maintainer').text,
                'maintainer_email': base_data.find('maintainer_email').text,
                'license_url': base_data.find('licence').text,
                'license_id': base_data.find('copyright').text,
                'translations': self._generate_term_translations(base_data, dataset),
                'resources': resources,
            }
        else:
            return None

    def info(self):
        return {
            'name': 'zhstat',
            'title': 'Statistical Office of Canton of Zurich',
            'description': 'Harvests the data of the Statistical Office of Canton of Zurich',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In ZhstatHarvester gather_stage')

        ids = []
        parser = etree.XMLParser(encoding='utf-8')

        for dataset in etree.fromstring(self._fetch_metadata(), parser=parser):

            # Get the german data if one is available, otherwise get the first one
            base_datas = dataset.xpath("data[@xml:lang='de']")
            if len(base_datas) != 0:
                base_data = base_datas[0]
            else:
                base_data = dataset.find('data')

            metadata = self._generate_metadata(base_data, dataset)

            if metadata:
                obj = HarvestObject(
                    guid = base_data.get('id'),
                    job = harvest_job,
                    content = json.dumps(metadata)
                )
                obj.save()
                log.debug('adding ' + base_data.get('id') + ' to the queue')
                ids.append(obj.id)
            else:
                log.debug('Skipping ' + base_data.get('id') + ' since no resources or groups are available')

        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In ZhstatHarvester fetch_stage')

    def import_stage(self, harvest_object):
        log.debug('In ZhstatHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        return True
