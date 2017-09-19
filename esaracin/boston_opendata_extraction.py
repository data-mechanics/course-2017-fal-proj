import urllib.request
import json
import dml
import prov.modl
import datetime
import uuid

class boston_opendata_extraction(dml.Algorithm):
    contributor = 'esaracin'
    reads = []
    writes = ['esaracin.police_stations', 'esaracin.police_districts']

    @staticmethod
    def execute(trial = False):
        '''Retrieves our data sets from Boston Open Data using specific URLs.
        Creates the necessary pymongo collections within our repo database.'''
        return

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return 
