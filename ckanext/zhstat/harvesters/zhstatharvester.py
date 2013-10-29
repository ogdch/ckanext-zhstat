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
from ckan.lib.munge import munge_title_to_name

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

    HARVEST_USER = 'harvest'

    BUCKET_NAME = 'bar-opendata-ch'
    DATA_PATH = 'Kanton-ZH/Statistik/'
    METADATA_FILE_NAME = 'metadata.xml'

    # Define the keys in the CKAN .ini file
    AWS_ACCESS_KEY = config.get('ckanext.zhstat.access_key')
    AWS_SECRET_KEY = config.get('ckanext.zhstat.secret_key')

    ORGANIZATION = {
        u'de': {
            'name': u'Kanton Zürich',
            'description': u'Im Rahmen eines Pilotversuchs veröffentlicht der Kanton Zürich ausgewählte Datensätze des Statistischen Amts und des GIS-ZH (Geografisches Informationssystem des Kantons Zürich).',
            'website': 'http://opendata.zh.ch',
        },
        u'fr': {
            'name': u'Canton de Zurich',
            'description': u"Dans le cadre d'un projet pilote le canton de Zurich publie des données sélectionnées de l'Office de la statistique et du GIS-ZH (Système d'information géographique du canton de Zurich).",
        },
        u'it': {
            'name': u'Cantone di Zurigo',
            'description': u"Come parte di un test pilota, il cantone di Zurigo pubblica dei dati selezionati dell'Ufficio statistico e del GIS-ZH (Sistema Informativo Territoriale del cantone di Zurigo)",
        },
        u'en': {
            'name': u'Canton of Zurich',
            'description': u"As part of a pilot test, the Canton of Zurich publishes selected data of the Statistical Office and of the GIS-ZH (Geographic Information System of the Canton of Zurich).",
        }
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
        '''Fetching the metadata file for for the Statistical Office of
        Canton of Zurich from the S3 Bucket and save on disk

        '''
        temp_dir = tempfile.mkdtemp()
        try:
            metadata_file = Key(self._get_s3_bucket())
            metadata_file.key = self.DATA_PATH + self.METADATA_FILE_NAME
            metadata_file_path = os.path.join(temp_dir, self.METADATA_FILE_NAME)
            log.debug('Saving metadata file to %s' % metadata_file_path)
            metadata_file.get_contents_to_filename(metadata_file_path)
            return open(metadata_file_path).read()
        except Exception, detail:
            log.exception(detail)
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
            log.debug('File does not exist on S3: ' + file_name)
            return False

    def _get_file_url(self, file_name):
        '''
        Generate a URL for the given S3 file name
        '''
        k = Key(self._get_s3_bucket())
        k.key = self.DATA_PATH + file_name
        return k.generate_url(0, query_auth=False, force_http=True)

    def _generate_tags_array(self, dataset):
        '''
        All tags for a dataset into an array
        '''
        tags = []
        try:
            for tag in dataset.find('tags').findall('tag'):
                tags.append(tag.text)
        except AttributeError:
            return tags

        return tags

    def _get_data_groups(self, data):
        '''
        Get group name
        '''
        groups = []

        try:
            for tag in data.find('groups').findall('group'):
                groups.append(tag.text)
        except AttributeError:
            return groups

        return groups

    def _generate_term_translations(self, base_data, dataset):
        '''
        Return all the term_translations for a given dataset
        '''
        translations = []

        for data in dataset:
            if base_data.find('title') != data.find('title'):
                lang = data.get('{http://www.w3.org/XML/1998/namespace}lang')
                for base_group, group in zip(self._get_data_groups(base_data), self._get_data_groups(data)):
                    translations.append({
                        'lang_code': lang,
                        'term': base_group,
                        'term_translation': group
                    })
                for base_tag, tag in zip(self._generate_tags_array(base_data), self._generate_tags_array(data)):
                    translations.append({
                        'lang_code': lang,
                        'term': munge_tag(base_tag),
                        'term_translation': munge_tag(tag)
                    })
                for key in ['title', 'author', 'maintainer', 'description']:
                    if base_data.find(key) is not None and data.find(key) is not None:
                        translations.append({
                            'lang_code': lang,
                            'term': base_data.find(key).text,
                            'term_translation': data.find(key).text
                            })
                for lang, org in self.ORGANIZATION.items():
                    if lang != u'de':
                        for field in ['name', 'description']:
                            translations.append({
                                'lang_code': lang,
                                'term': self.ORGANIZATION[u'de'][field],
                                'term_translation': org[field]
                            })

        return translations

    def _generate_resources(self, dataset):
        '''
        Return all resources for a given dataset that are available
        '''
        resources = []
        for data in dataset:
            if data.find('resources') is not None:
                for resource in data.find('resources'):
                    if self._file_is_available(resource.find('name').text):
                        resources.append({
                            'url': self._get_file_url(resource.find('name').text),
                            'name': resource.find('name').text,
                            'format': resource.find('type').text,
                            'description': resource.find('description').text if resource.find('description') is not None else ''
                        })

        return resources

    def _generate_metadata(self, base_data, dataset):
        '''
        Return all the necessary metadata to be able to create a dataset
        '''
        resources = self._generate_resources(dataset)
        groups = self._get_data_groups(base_data)

        if len(resources) != 0 and groups:
            return {
                'datasetID': dataset.get('id'),
                'url': base_data.find('url').text,
                'title': base_data.find('title').text,
                'author': base_data.find('author').text,
                'author_email': base_data.find('author_email').text,
                'notes': base_data.find('description').text,
                'maintainer': base_data.find('maintainer').text,
                'maintainer_email': base_data.find('maintainer_email').text,
                'license_url': base_data.find('license').get('url'),
                'license_id': base_data.find('license').text,
                'translations': self._generate_term_translations(base_data, dataset),
                'resources': resources,
                'tags': self._generate_tags_array(base_data),
                'groups': groups
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
                    guid = dataset.get('id'),
                    job = harvest_job,
                    content = json.dumps(metadata)
                )
                obj.save()
                log.debug('adding ' + dataset.get('id') + ' to the queue')
                ids.append(obj.id)
            else:
                log.debug('Skipping ' + dataset.get('id') + ' since no resources or groups are available')

        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In ZhstatHarvester fetch_stage')

        dataset_id = json.loads(harvest_object.content)['datasetID']
        log.debug(harvest_object.content)

        try:
            harvest_object.save()
            log.debug('successfully processed ' + dataset_id)
            return True
        except Exception, detail:
            log.exception(detail)
            raise

    def import_stage(self, harvest_object):
        log.debug('In ZhstatHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        try:
            package_dict = json.loads(harvest_object.content)

            package_dict['id'] = harvest_object.guid
            package_dict['name'] = self._gen_new_name(package_dict['title'])

            user = model.User.get(self.HARVEST_USER)
            context = {
                'model': model,
                'session': Session,
                'user': self.HARVEST_USER
                }

            # Find or create group the dataset should get assigned to
            for group_name in package_dict['groups']:
                if not group_name:
                    raise GroupNotFoundError('Group is not defined for dataset %s' % package_dict['title'])
                data_dict = {
                    'id': group_name,
                    'name': self._gen_new_name(group_name),
                    'title': group_name
                    }
                try:
                    group_id = get_action('group_show')(context, data_dict)['id']
                except:
                    group = get_action('group_create')(context, data_dict)
                    log.info('created the group ' + group['id'])

            # Find or create the organization the dataset should get assigned to
            data_dict = {
                'permission': 'edit_group',
                'id': munge_title_to_name(self.ORGANIZATION[u'de']['name']),
                'name': munge_title_to_name(self.ORGANIZATION[u'de']['name']),
                'title': self.ORGANIZATION[u'de']['name'],
                'extras': [
                    {
                        'key': 'website',
                        'value': self.ORGANIZATION[u'de']['website']
                    }
                ]
            }
            try:
                package_dict['owner_org'] = get_action('organization_show')(context, data_dict)['id']
            except:
                organization = get_action('organization_create')(context, data_dict)
                package_dict['owner_org'] = organization['id']


            # Save additional metadata in extras
            extras = []
            if 'license_url' in package_dict:
                extras.append(('license_url', package_dict['license_url']))
            package_dict['extras'] = extras
            log.debug('Extras %s' % extras)

            package = model.Package.get(package_dict['id'])
            pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            self._create_or_update_package(package_dict, harvest_object)

            # Add the translations to the term_translations table
            for translation in package_dict['translations']:
                action.update.term_translation_update(context, translation)
            Session.commit()

        except Exception, detail:
            log.exception(detail)
            raise

        return True


class GroupNotFoundError(Exception):
    pass
