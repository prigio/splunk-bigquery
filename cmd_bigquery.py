"""
Splunk search command executing SQL query on GCP BigQuery
"""
import os
import sys
import json
import errno
import time
import hashlib

sys.path.insert(0,'./lib')

from splunklib.searchcommands import dispatch, GeneratingCommand, Configuration, Option, validators
from splunklib.client import KVStoreCollection
#from splunklib.binding import HTTPError
from google.cloud import bigquery


@Configuration(type='reporting', distributed=False)
class BigQueryCommand(GeneratingCommand):
    """ %(synopsis)

    ##Syntax

    %(syntax)

    ##Description

    %(description)

    """
    def __init__(self):
        super(BigQueryCommand, self).__init__()

    query = Option(doc='''
        **Syntax:** **query=***<sql>*
        **Description:** SQL query to execute.''',
        name="query",
        require=True,
    )
    timeout = Option(doc='''
        **Syntax:** **query=***<int>*
        **Description:** Max time to wait for query to complete.''',
        name="timeout",
        require=False,
        default=60,
        validate=validators.Integer(minimum=0)
        )
    cache = Option(doc='''
        **Syntax:** **cache=***<int>*
        **Description:** Set to an integer to say for how many seconds should the results of the query be cached.''',
        name="cache",
        require=False,
        validate=validators.Integer(minimum=0)
        )

    def prepare(self):
        #if self.process_name == None and self.collection == None:
        #    self.write_error("processstats: either a collection or a process stanza name should be provided")
        #    sys.exit(1)
        try:
            if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
                raise Exception("env variable GOOGLE_APPLICATION_CREDENTIALS not specified. It should point to a GCP configuration file. You can set this in splunk-launch.conf")
            
            if not os.path.isfile(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
            
            self.cache_kvstore = self.service.kvstore['bigquery_cache']
            # delete expired cache data
            self.cache_kvstore.data.delete(query=json.dumps({"_expiration_ts": {"$lt": time.time() }}))
            
            self.timeout = int(float(self.timeout))
            self.query_hash = hashlib.md5(self.query.encode("utf-8")).hexdigest()

            if self.cache not in (None, ''):
                self.cache = int(float(self.cache))
            
        except Exception as e:
            self.write_error(f"bigquery: {type(e)} - {str(e)}")
            sys.exit(3)

    
    def get_from_cache(self):
        requested_records = 5000
        tot_retrieved = 0
        more_records = True
        while more_records:
            kv_records = self.cache_kvstore.data.query(limit=requested_records,
                        skip=tot_retrieved, 
                        sort="_key",
                        query=json.dumps({"_query_hash": self.query_hash}))
            tot_retrieved += len(kv_records)
            if len(kv_records) < requested_records:
                more_records = False
            for r in kv_records:
                yield r
    
    @Configuration(run_in_preview=False)
    def generate(self):
        i=0
        try:
            start_time = time.time()
            # if caching is configured
            if self.cache not in (None, ''):
                # look for data within cache and output it
                for row in self.get_from_cache():
                    i+=1
                    yield(row)
                # served data out of cache, no need to actually do anything else
                if i>0:
                    # if there was data within cache, it's done
                    self.write_info(f"bigquery: data retrived from cache in {round(time.time() - start_time,3)}s")
                    return
            
            # there was cachinig configured, or no data in cache
            self.client = bigquery.Client()
            query_job = self.client.query(self.query, timeout=self.timeout)
            tot_wait=0
            while query_job.running() and tot_wait<self.timeout:
                time.sleep(1)
                tot_wait+=1
            if tot_wait>self.timeout and not query_job.done():
                self.write_error("bigquery: timeout expired")
                return
            
            if query_job.error_result not in (None, {}):
                err = query_job.error_result
                self.write_error(f"bigquery: {err['reason']} - {err['message']}")
                return

            if self.cache in (None, ''):
                for row in query_job:
                    i+=1
                    yield(dict(row))
                self.write_info(f"bigquery: data retrieved from bigquery in {round(time.time() - start_time,3)}s")
            else:
                data_to_be_cached = []
                expiration_ts = time.time() + self.cache

                for row in query_job:
                    i+=1
                    d = dict(row)
                    yield(d)
                    d['_expiration_ts'] = expiration_ts
                    d['_query_hash'] = self.query_hash
                    data_to_be_cached.append(d)
                    if len(data_to_be_cached) > 100:
                        self.cache_kvstore.data.batch_save(*data_to_be_cached)
                        data_to_be_cached = []
                if len(data_to_be_cached) > 0:
                    self.cache_kvstore.data.batch_save(*data_to_be_cached)
                    data_to_be_cached = []
                self.write_info(f"bigquery: data retrieved from bigquery and stored in cache in {round(time.time() - start_time,3)}s")
        except Exception as e:
            self.write_fatal(f"bigquery: {type(e)} - {str(e)}")
        finally:
            if i==0: yield {}

dispatch(BigQueryCommand, sys.argv, sys.stdin, sys.stdout, __name__)
