import ckan
import ckan.plugins as p
from pylons import config

class ZhstatHarvest(p.SingletonPlugin):
    """
    Plugin containing the harvester for for the Statistical Office of Canton of Zurich
    """
