#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import urllib
import cgi
import pprint
import time
import os

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import util
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.api import app_identity

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
#from mapreduce import operation as op
#from mapreduce import shuffler

class Person(db.Model):
  name = db.StringProperty()

class Result(db.Model):
  blob_key = blobstore.BlobReferenceProperty()
  url = db.LinkProperty()

class PipelineHandler(webapp.RequestHandler):
    def get(self):
        db.delete(Person.all())

        for dummy in range(1,3):
          tom = Person(name="tom")
          dick = Person(name="dick")
          harry = Person(name="harry")
          db.put([tom, dick, harry])

        results = Result.all()
        for result in results:
          self.response.out.write('%s: %s<br>' % (result.key(), result.blob_key) )

        self.response.out.write('-----------------------------------------------<br>')

        people = Person.all()
        for person in people:
          self.response.out.write('%s: %s<br>' % (person.key(), person.name) )
        pipeline = MyPipeline(Person.kind())
        pipeline.start()

        self.response.out.write('Done.<br>')

class MyPipeline(base_handler.PipelineBase):

  def run(self, entity_kind):
    logging.debug("entity_kind is %s" % entity_kind)
    output = yield mapreduce_pipeline.MapreducePipeline(
        "My MapReduce",
        "main.my_map",
        "main.my_reduce",
        "mapreduce.input_readers.DatastoreEntityInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
        mapper_params={
            "entity_kind": entity_kind,
        },
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=1)
    yield StoreOutput("MyPiplineOutput", entity_kind, output)

def my_map(data):
  """My map function."""

  name = data["name"]
  logging.info("----------------------------------- Got %s: %s" % (data.kind(), name) )
  yield(name, 1)

def my_reduce(key, values):
  """Word Count reduce function."""
  yield "--------------------------------------------------------\nCount: %d\nMessage:\n%s\n" % (len(values), key)

class StoreOutput(base_handler.PipelineBase):
  """A pipeline to store the result of the MapReduce job in the datastore.
  """

  def run(self, mr_kind, entity_kind, blob_keys):
    for blob_key in blob_keys: 
      logging.info("********************************************** %s output for %s: %s" % (mr_kind, entity_kind, blob_key) )
      url = "http://%s%s" % (app_identity.get_default_version_hostname(), blob_key)
      db.put(Result(blob_key=blob_key, url=url))


class DownloadHandler(blobstore_handlers.BlobstoreDownloadHandler):
  """Handler to download blob by blobkey."""

  def get(self, key):
    key = str(urllib.unquote(key)).strip()
    logging.debug("key is %s" % key)
    blob_info = blobstore.BlobInfo.get(key)
    self.send_blob(blob_info)

APP = webapp.WSGIApplication(
    [
        ('/', PipelineHandler),
        (r'/blobstore/(.*)', DownloadHandler),
    ],
    debug=True)

def main():
  logging.getLogger().setLevel(logging.DEBUG)
  util.run_wsgi_app(APP)


if __name__ == '__main__':
    main()
