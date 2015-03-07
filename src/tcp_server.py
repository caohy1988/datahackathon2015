#!/usr/bin/python
"""
A basic gevent tcp server with worker processes
"""
import argparse
import copy
import gevent
import gevent.server
import simplejson as json
import math
import multiprocessing
import os
import random
import sys
import urlparse


def make_handle(workers_list):
  """make the handler function, which wants to know about the workers"""
  def handle(socket, _):
    """
    Handle an incoming connection
    n.b. this may contain multiple lines so
    greedily read off socket until it's empty
    """
    fileobj = socket.makefile()
    while True:
      line = fileobj.readline()
      if not line:
        break
      resp = process_line(line, filehandle=fileobj, workers=workers_list)
      if resp: # if there's something to send back to the client
        fileobj.write(resp + '\n') # then do that
        fileobj.close()
        break
  return handle


def process_line(line, filehandle=None, workers=None):
  """Handle a single line, this needs to determine"""
  try: # do this if you get a json line
    complaint = json.loads(line.strip())
    return enqueue(complaint, workers=workers)
  except ValueError: # do this if you get a non-json line
    return process_query_line(line.strip(), filehandle=filehandle, workers=workers)


def process_query_line(line, filehandle=None, workers=None):
  """Handle a query line"""
  parsed = urlparse.urlparse(line)
  action = parsed.path.rpartition('/')[-1]
  my_func = globals().get(action, None)
  if my_func is None:
    resp = 'Processor for "{}" not yet implemented!'.format(action)
    print >> sys.stderr, resp
    return resp
  else:
    query_string_dict = dict(urlparse.parse_qsl(parsed.query))
    return my_func(query_string_dict, filehandle=filehandle, workers=workers)


def enqueue(complaint, workers=None): # args are line_dict, line, workers
  """Send a single data line to the appropriate worker."""
  complaint['location'] = get_location(complaint, bin_granularity=50.0)
  worker = worker_getter(complaint, workers)
  worker.inqueue.put(complaint)


def scatter(complaint, workers):
  return random.choice(workers)


def by_data(complaint, workers):
  worker_id = hash(complaint.get(HIERARCHY_KEYS[0])) % len(workers)
  return workers[worker_id]


def two_level(complaint, workers):
  # idea is to have worker i, i+1 each have half of the same sample of data
  bucket_id = (hash(complaint.get(HIERARCHY_KEYS[0])) % len(workers)) // 2
  worker_id = 2 * bucket_id + (hash(complaint.get('Unique Key')) % 2)
  return workers[worker_id]


def merge_dicts(dicta, dictb):
  """merge two dictionaries, summing leaf nodes"""
  ans = dict()
  if type(dicta) in [type(0)]:
    return sum([dicta, dictb])
  all_keys = set(dictb.iterkeys())
  all_keys.update(dicta.iterkeys())
  for key in all_keys:
    if key not in dicta: # handle the keys b only
      ans[key] = copy.deepcopy(dictb[key])
    elif key not in dictb: # a only
      ans[key] = copy.deepcopy(dicta[key])
    else: # both
      ans[key] = merge_dicts(dicta[key], dictb[key])
  return ans


def get(query_string_dict, filehandle=None, workers=None):
  """get the dictionaries from the worker processes, rolled up into one"""
  results = dict()
  query_string_dict['action'] = 'get'
  for worker in workers:
    worker.inqueue.put(query_string_dict)
  for worker in workers:
    iresult = worker.outqueue.get(True)
    results = merge_dicts(results, iresult)
  return json.dumps(results, indent=4, sort_keys=True)


def get_data(query_string_dict, filehandle=None, workers=None):
  """get the dictionaries from the worker processes, rolled up into one"""
  results = list()
  query_string_dict['action'] = 'get'
  for worker in workers:
    worker.inqueue.put(query_string_dict)
  for worker in workers:
    iresult = worker.outqueue.get(True)
    results.append(iresult)
  return json.dumps(results, indent=4, sort_keys=True)


def get_counts(query_string_dict, filehandle=None, workers=None):
  """get an array of the number of lines processed by each worker"""
  results = list()
  query = dict()
  query['action'] = 'get_counts'
  for worker in workers:
    worker.inqueue.put(query)
  for worker in workers:
    results.append(worker.outqueue.get(True))
  result = dict()
  result['by_worker'] = results
  result['total'] = sum(results)
  return json.dumps(results, indent=4, sort_keys=True)


def get_cardinality(query_string_dict, filehandle=None, workers=None):
  """get an array of the number of leaf nodes in each worker's dictionary"""
  results = list()
  query = dict()
  query['action'] = 'get_cardinality'
  for worker in workers:
    worker.inqueue.put(query)
  for worker in workers:
    results.append(worker.outqueue.get(True))
  return json.dumps(results)


def say_hi(query_string_dict, filehandle=None, workers=None):
  """ask each of the workers to say hello"""
  results = list()
  query = dict()
  query['action'] = 'say_hi'
  for worker in workers:
    worker.inqueue.put(query)
  for worker in workers:
    results.append(worker.outqueue.get(True))
  return json.dumps(results, indent=4, sort_keys=True)


def get_event_id(line_dict):
  """get as close as possible to a unique id for an event"""
  event_id = line_dict.get('Unique Key')
  if event_id is not None:
    return event_id
  fallback_key = (line_dict.get('Created Date'), line_dict.get('Complaint Type'))
  return hash(fallback_key)


def get_id(idata):
  """get a pseudo unique representation of an event"""
  attributes = ['Complaint Type', 'Agency', 'Location Type', 'Descriptor', 'Incident Zip']
  as_list = list()
  for attr in attributes:
    as_list.append(idata.get(attr))
  return tuple(as_list)


def quit(_, **kwargs):
  """send the exit signal to all workers and close up shop"""
  message = "Received hangup signal, exiting."
  if kwargs.get('filehandle'):
    print >> kwargs.get('filehandle'), message
  if kwargs.get('workers'):
    for worker in kwargs.get('workers'):
      worker.inqueue.put(None)
    for worker in kwargs.get('workers'):
      worker.join()
  print >> sys.stderr, message
  sys.exit(0)


class worker_class(multiprocessing.Process):
  """the worker class"""
  def __init__(self, iqueue, rqueue, worker_id=None, stderr_lock=multiprocessing.RLock()):
    """make a worker"""
    self.inqueue = iqueue
    self.outqueue = rqueue
    self.worker_id = worker_id
    self.stderr_lock = stderr_lock
    self.lines_processed = 0
    self.my_data = dict()
    self.seen_events = set() # bloom filter lives here
    super(worker_class, self).__init__()

  def run(self):
    """workers will do this until they get the hangup signal"""
    with self.stderr_lock:
      print >> sys.stderr, self.say_hi(dict())
    while True:
      try:
        next_data = self.inqueue.get(True) # make a blocking get
      except KeyboardInterrupt:
        next_data = None
      if next_data is None: # this is my code to hang up
        with self.stderr_lock:
          print >> sys.stderr, "Goodbye from worker {}".format(self.worker_id)
        break
      if 'action' not in next_data:
        self.worker_process_data(next_data)
      else:
        function = getattr(self, next_data['action'])
        self.outqueue.put(function(next_data))

  def get_counts(self, query_string_dict):
    return self.lines_processed

  def get_cardinality(self, query_string_dict):
    return count_leaf_nodes(self.my_data)

  def say_hi(self, query_string_dict):
    return "Hello from worker {}, pid = {}".format(self.worker_id, os.getpid())

  def get(self, query_string_dict):
    return self.my_data

  def worker_process_data(self, idata):
    """how a worker deals with a single chunk of data"""
    idata['location'] = str(get_location(idata))
    event_id = get_id(idata)
    if event_id in self.seen_events:
      return
    self.seen_events.add(event_id)
    self.lines_processed += 1
    levels = list()
    dct_pointer = self.my_data
    for key in HIERARCHY_KEYS[:-1]:
      keyval = idata.get(key)
      if type(keyval) in [type(""), type(u"")]:
        keyval = str(keyval.title())
      dct_pointer.setdefault(keyval, dict())
      dct_pointer = dct_pointer[keyval]
    keyval = idata.get(HIERARCHY_KEYS[-1])
    if type(keyval) in [type(""), type(u"")]:
      keyval = str(keyval.title())
    dct_pointer.setdefault(keyval, 0)
    dct_pointer[keyval] += 1
    # just spend some cpu cycles, usually you'd expect
    # to have to do some more business logic here
    for i in xrange(20000):
      _ = math.sqrt(i)


def count_leaf_nodes(idict):
  if type(idict) in [type(dict())]:
    return sum((count_leaf_nodes(x) for x in idict.itervalues()))
  else:
    return 1


def get_location(complaint, bin_granularity=50.0):
  """bin the location with some sort of granularity"""
  ilat = float(complaint.get('Latitude', 0))
  ilong = float(complaint.get('Longitude', 0))
  ilat = round(bin_granularity * ilat) / bin_granularity
  ilong = round(bin_granularity * ilong) / bin_granularity
  return tuple([ilat, ilong])


# HIERARCHY_KEYS = ['location', 'Complaint Type', 'Descriptor']
# HIERARCHY_KEYS = ['location']
HIERARCHY_KEYS = ['Complaint Type']

def main():
  """The mainsy"""
  parser = argparse.ArgumentParser()
  parser.add_argument('--router', type=str, choices=['random', 'by_data', 'two_level'], required=True)
  parser.add_argument('PORT_NUM', type=int)
  parser.add_argument('--num-workers', type=int, default=multiprocessing.cpu_count())
  args = vars(parser.parse_args())
  if 'random' == args['router']:
    args['worker_getter'] = scatter
  else:
    args['worker_getter'] = globals()[args['router']]
  globals().update(args) # copy the args directly into globals (there are probably loads of good reasons to not do this in practice)
  # spawn the workers
  STDERR_LOCK = multiprocessing.RLock()
  workers_list = list()
  for i in xrange(args['num_workers']):
    worker_queue = multiprocessing.Queue()
    results_queue = multiprocessing.Queue()
    iworker = worker_class(worker_queue, results_queue, worker_id=i, stderr_lock=STDERR_LOCK)
    workers_list.append(iworker)
    with STDERR_LOCK:
      print >> sys.stderr, "Starting worker {}".format(i)
    iworker.start() # get_cardinality
  print >> sys.stderr, "Starting tcp server."
  handle = make_handle(workers_list)
  server = gevent.server.StreamServer(('0.0.0.0', PORT_NUM), handle) # creates a new server
  try:
    server.serve_forever() # start accepting new connections
  except KeyboardInterrupt:
    sys.exit(1)


if "__main__" == __name__:
  main()
