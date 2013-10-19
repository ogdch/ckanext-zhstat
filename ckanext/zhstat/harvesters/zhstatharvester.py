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

    BUCKET_NAME = u'bar-opendata-ch'
    METADATA_FILE_NAME = u'metadata.xml'
    FILES_BASE_URL = u'http://bar-opendata-ch.s3.amazonaws.com/Kanton-ZH/Statistik'

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

    def _get_s3_bucket(self):
        '''
        Create an S3 connection to the department bucket
        '''
        if self.bucket is None:
            conn = S3Connection(self.AWS_ACCESS_KEY, self.AWS_SECRET_KEY)
            self.bucket = conn.get_bucket(self.BUCKET_NAME)
        return self.bucket


    def _fetch_metadata_file(self):
        '''
        Fetching the Excel metadata file for for the Statistical Office of Canton of Zurich from the S3 Bucket and save on disk
        '''
        temp_dir = tempfile.mkdtemp()
        try:
            metadata_file = Key(self._get_s3_bucket())
            metadata_file.key = self.METADATA_FILE_NAME
            metadata_file_path = os.path.join(temp_dir, self.METADATA_FILE_NAME)
            log.debug('Saving metadata file to %s' % metadata_file_path)
            metadata_file.get_contents_to_filename(metadata_file_path)
            return metadata_file_path
        except Exception, e:
            log.exception(e)
            raise


    def info(self):
        return {
            'name': 'zhstat',
            'title': 'Statistical Office of Canton of Zurich',
            'description': 'Harvests the data of the Statistical Office of Canton of Zurich',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In ZhstatHarvester gather_stage')
        try:
            file_path = self._fetch_metadata_file()
            ids = []

            tree = etree.parse(file_path)
            for dataset in tree.findall('dataset'):

                log.debug(etree.tostring(dataset.find('data').find('resources').find('resource').find('name')))
                log.debug(etree.tostring(dataset))

                metadata = {
                    'datasetID': dataset.get('id'),
                    'title': 'testme',
                    'notes': 'hcehucehu',
                    'author': 'foobar',
                    'maintainer': 'hagsdkfjhag',
                    'maintainer_email': 'jahdfk@jsdgfj.cs',
                    'license_id': 'ahdfgkajshdf',
                    'tags': [],
                    'groups': []
                }

                obj = HarvestObject(
                    guid = metadata['datasetID'],
                    job = harvest_job,
                    content = json.dumps(metadata)
                )
                obj.save()
                log.debug('adding ' + metadata['datasetID'] + ' to the queue')
                ids.append(obj.id)

        except Exception, e:
            log.debug(e)
            return False
        return ids


    def fetch_stage(self, harvest_object):
        log.debug('In ZhstatHarvester fetch_stage')

    def import_stage(self, harvest_object):
        log.debug('In ZhstatHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        return True
