#!/usr/bin/python
import multiprocess as mp
import threading
from Queue import Queue
from kafka import KafkaConsumer
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import IndicesClient
import logging
import signal
import socket
import sys
import cjson
import time, datetime
import ConfigParser
from functools import partial
from os import getpid
import geoip2.database
import psutil

"""
This is Smonnet (Sflow Monitor Network) the program the works as:

* Kafka Consumer
* Data Segregator
* Data Processor
* Elasticsearch Indexer (Data Propagator)

It can be used in a multitenant environment where each user has its own isolated space, a separate process with a specific kafka group ID which sends to a specific Elasticsearch cluster.

This product includes GeoLite2 data created by MaxMind, available from
`www.maxmind.com <http://www.maxmind.com>`.

"""

class Initialize:
  """
  This class is used to initialize the connections to the Kafka cluster and to Elasticsearch cluster.

  It includes the following methods:

  * :func:`SetupES`
  * :func:`SetupConsumer`

  """
  def SetupES(self, escluster):
    """
    This is the method that initializes the connection with Elasticsearch cluster.

    :param escluster: the hosts and ports of the cluster with format <host>:<port>
    :type escluster: list
    :raises: Exception - when the connection is not possible to be initialized (possibly *ConnectionTimeout*)
    """

    #TODO include the username and password of the ES cluster where necessary
    loggerIndex.info('Connecting to Elasticsearch cluster')
    try:
      es = Elasticsearch(escluster,
            http_auth=('elastic', 'changeme'),
            #sniff_on_start=True,
            #sniff_on_connection_fail=True,
            #sniffer_timeout=120
            )
      return es
    except Exception, e:
      loggerConsumer.error("During Elasticsearch cluster instantiation: %s" %e)

  def SetupConsumer(self, groupId):
    """
    This method is responsible for initializing the connection with Kafka cluster.

    :param groupId: the groupID of the kafka consumer which is a different subscriber-user
    :type groupId: str
    :raises: Exception - when there is a problem with consumer instantiation
    """

    #TODO read bootstrap servers from arguments or from config file
    # To consume latest messages and auto-commit offsets
    loggerConsumer.info('Consumer group %s starts' %(groupId))
    try:
      myConsumer = KafkaConsumer('sflow2',
                              group_id=groupId,
                              bootstrap_servers=['smonnet-brpr1:9092','smonnet-brpr2:9092'],
                              max_partition_fetch_bytes=200000,
                              partition_assignment_strategy=[RoundRobinPartitionAssignor])
      return myConsumer
    except Exception, e:
      loggerConsumer.error("During consumer instantiation: %s" %e)

class MonConfig(Initialize):
  """
  This class is to configure each user of the program through *api.cfg*.

  It includes the following methods:

  * :func:`ClusterConnections`
  * :func:`FilterOutParams`

  """

  def ClusterConnections(self):
    """
    This method reads Elasticsearch hosts from the config file the *monitor_nodes* field of the *smonit* section and then calls :funk:`Initialize.SetupES` method to instantiate the connection with Elasticsearch.

    .. note::

      The acceptable format for *monitor_nodes* field is 
      *monitor_nodes = <host1>:<port1>,<host2>:<port2>--<host3>:<port3>,...*

    """

    esConnections = []
    monitor_nodes = config.get('smonit','monitor_nodes').split('--')
    for node in monitor_nodes:
      instance_node = node.split(',')
      mon_instance_init = Initialize()
      mon_instance = mon_instance_init.SetupES(instance_node)
      esConnections.append(mon_instance)
    return esConnections

  def FilterOutParams(self):
    """
    This method reads the filters that are given from the config the *monitor_params* field of the *smonit* section in order to segregate the data for each user.

    .. note::

      The acceptable fromat for *monitor_params* field is
      *monitor_params = <sFlow_field>,<sFlow_field_value1>-<sFlow_field>,<sFlow_field_value2>

    """

    #TODO support more filters for each user
    filterParams = []
    monitor_params_str = config.get('smonit', 'monitor_params')
    if monitor_params_str:
      monitor_params = monitor_params_str.split('-')
    else:
      monitor_params = []
    for param in monitor_params:
      cluster_params = param.split(',')
      filterParams.append((cluster_params[0], cluster_params[1]))
    return filterParams

class myThread(threading.Thread):
  """
  This class is used to initialize and run threads and it inherits the methods from the normal *threading.Thread* class.

  It includes the following methods:

  * :func:`__init__`
  * :func:`run`

  """
  def __init__(self, queue, name, es):
    """
    This method instantiates all the variables that are needed to run a daemon thread.

    :param queue: The queue where all the tasks exist and threads consume from it
    :type queue: Queue
    :param name: the name of the thread
    :type name: str
    :param es: the Elasticsearch object with the information about the connection to the cluster
    :type es: elasticsearch.Elasticsearch

    """

    threading.Thread.__init__(self)
    self.queue = queue
    self.name = name
    self.es = es

  def run(self):
    """
    This method is called when a thread starts.

    It gets the first batch of data from the queue and it sends it to the given Elasticsearch cluster. If a document failed to be sent an error message will appear.
    """

    while True:
      send_data = self.queue.get()
      loggerConsumer.info("Sending to %s from thread %s" %(self.es,threading.currentThread().getName()))
      for success, info in helpers.parallel_bulk(self.es, send_data, chunk_size=4000, thread_count=4, request_timeout=30):
        if not success:
          loggerConsumer.error("A document failed: %s" %(info))
      self.queue.task_done()
      loggerConsumer.debug("batch done, %s, %s" %(self.queue.qsize(), self.queue.empty()))


class MessageHandler():
  """
  This class handles the messages from Kafka.

  It includes all the functions that are needed to process, segregate the network traffic and then send it to the apropriate Elasticsearch cluster.

  It includes the following methods:

  * :func:`__init__`
  * :func:`Accept`
  * :func:`EmbedData`
  * :func:`tryASip`
  * :func:`tryGeoIP`
  * :func:`mapIP`

  """

  def __init__(self, es, filter_params):
    """
    This method instantiates all the necessary variables for handling the messages from Kafka, process them and then send them to the apropriate Elasticsearch cluster.

    * It writes the pid of each child process to the *api.cfg*
    * It sends the *template.json* to the Elasticsearch with all the right mappings for being able to be detected and visualized by Elasticsearch
    * It sets the size of the batch of data to send to Elasticsearch
    * It sets the number of parts of each batch has to be divided in order to send them in parallel through threads
    * It starts as many daemon threads as the number of parts it was set before
    * It loads the datasets for the Geo IP and AS enrichment

    :param es: the Elasticsearch object with the information about the connection to the cluster
    :type es: elasticsearch.Elasticsearch
    :param filter_params: it includes the sFlow field and its value that is needed to be filtered in
    :type filter_params: list
    """

    child_procs = config.get('smonit', 'child_procs')
    if child_procs:
      child_procs =  child_procs + ',' + str(getpid())
    else:
      child_procs = getpid()
    with open(config_file, 'w') as fd:
      config.set('smonit', 'child_procs', child_procs)
      config.write(fd)

    with open('sproc/template.json') as fd:
      IndicesClient(es).put_template('sflow_template', body=json.load(fd))
    self.data = dict()
    self.send_data = list()
    self.es = es
    self.filter_params = filter_params

    self.queue = Queue()
    self.batch = 10000
    self.batch_parts = 4
    for i in range(self.batch_parts):
      self.push = myThread(self.queue, 'ThreadNo'+str(i), es)
      self.push.setDaemon(True)
      self.push.start()

    self.geoip = geoip2.database.Reader('sproc/GeoLite2-City.mmdb')
    self.ASip = geoip2.database.Reader('sproc/GeoLite2-ASN.mmdb')

  def Accept(self, body):
    """
    This method works as the callback of the Kafka Consumer.

    Particularly, it sends the messages from Kafka to be processed and then it handles the way to send send them to Elasticsearch. For this it needs to make a batch of messages which then are divided in smaller batches and they are sent to the ES cluster by threads.

    :param body: the body of the message which is consumed by Kafka Consumer client
    :type body: str

    """

    try:
      self.EmbedData(body)
    except Exception, e:
      loggerConsumer.error('Discarding message - failed to append to payload: %s' % e)

    if len(self.send_data) >= self.batch:
      send_data = filter(None, self.send_data)
      self.send_data = []
      if send_data:
        qs = self.queue.qsize()
        if qs < self.batch_parts:
          batch_step = self.batch/self.batch_parts
          for i in range(self.batch_parts):
            self.queue.put(send_data[batch_step*i:batch_step*i+batch_step])
        else:
          loggerConsumer.debug("FULL, queue size %s" %(qs))
          #TODO check if this kind of joining the tasks of a queue actually helps
          if qs % 2 == 0:
            joinThread = threading.Thread(target=self.queue.join)
            joinThread.start()
          else:
            self.queue.join()
          self.queue.put(send_data)

  def EmbedData(self, body):
    """
    This method parses each message from Kafka and it processes it. If theapropriate field matches the filters given then it enriches the message using the datasets, else it doesn't append the list with the batch (which is going to be sent to the Elasticsearch cluster).

   :param body: the body of the message which is consumed by Kafka Consumer client
   :type body: str

    .. note::

      For the enrichment the maxmind python libraries are required as well as the system library for faster lookup.

    """

    sflowSample = dict()
    timestamp = int(time.time() * 1000)

    fields = body.split(',')
    if fields[0] == "FLOW":
      sflow_ReporterIP = fields[1]
      sflow_inputPort = fields[2]
      sflow_outputPort = fields[3]
      sflow_srcMAC = fields[4]
      sflow_dstMAC = fields[5]
      sflow_EtherType = fields[6]
      sflow_srcVlan = fields[7]
      sflow_dstVlan = fields[8]
      sflow_srcIP = fields[9]
      sflow_dstIP = fields[10]
      try:
        socket.inet_pton(socket.AF_INET, sflow_srcIP)
      except:
        sflow_srcIP = '0.0.0.0'
      try:
        socket.inet_pton(socket.AF_INET, sflow_dstIP)
      except:
        sflow_dstIP = '0.0.0.0'
      sflow_IP_Protocol = fields[11]
      sflow_IPTOS = fields[12]
      sflow_IPTTL = fields[13]
      sflow_srcPort = fields[14]
      sflow_dstPort = fields[15]
      sflow_tcpFlags = fields[16]
      sflow_PacketSize = fields[17]
      sflow_IPSize = fields[18]
      sflow_SampleRate = fields[19]
      try:
        sflow_counter = fields[20]
      except:
        sflow_counter = -1

      [sflow_inputPort,sflow_outputPort,sflow_srcVlan,sflow_dstVlan,sflow_IP_Protocol,sflow_IPTTL,sflow_srcPort,sflow_dstPort,sflow_PacketSize,sflow_IPSize,sflow_SampleRate,sflow_SampleRate] = map(int, [sflow_inputPort,sflow_outputPort,sflow_srcVlan,sflow_dstVlan,sflow_IP_Protocol,sflow_IPTTL,sflow_srcPort,sflow_dstPort,sflow_PacketSize,sflow_IPSize,sflow_SampleRate,sflow_SampleRate])

      sflowSample = {
      '@message':body,
      '@timestamp':timestamp,
      '@version':1,
      'type':'sflow',
      'SampleType':'FLOW',
      'sflow_ReporterIP':sflow_ReporterIP,
      'sflow_inputPort':sflow_inputPort,
      'sflow_outputPort':sflow_outputPort,
      'sflow_srcMAC':sflow_srcMAC,
      'sflow_dstMAC':sflow_dstMAC,
      'sflow_EtherType':sflow_EtherType,
      'sflow_srcVlan':sflow_srcVlan,
      'sflow_dstVlan':sflow_dstVlan,
      'sflow_srcIP':sflow_srcIP,
      'sflow_dstIP':sflow_dstIP,
      'sflow_IP_Protocol':sflow_IP_Protocol,
      'sflow_IPTOS':sflow_IPTOS,
      'sflow_IPTTL':sflow_IPTTL,
      'sflow_srcPort':sflow_srcPort,
      'sflow_dstPort':sflow_dstPort,
      'sflow_tcpFlags':sflow_tcpFlags,
      'sflow_PacketSize':sflow_PacketSize,
      'sflow_IPSize':sflow_IPSize,
      'sflow_SampleRate':sflow_SampleRate,
      }
      if str(sflowSample.get(self.filter_params[0])) == self.filter_params[1]:
        [sflow_NEWsrcIP, sflow_NEWdstIP] = map(self.mapIP, [sflow_srcIP,sflow_dstIP])
        [src_details, dst_details] = map(self.tryGeoIP, [sflow_srcIP,sflow_dstIP])
        [src_as, dst_as] = map(self.tryASip, [sflow_srcIP,sflow_dstIP])
        if src_details:
          sflowSample.update({'sflow_src_location': {
                                'lat': src_details.location.latitude,
                                'lon': src_details.location.longitude
                             },
                             'sflow_src_country': src_details.country.name
                            })

        if src_as:
          sflowSample.update({'sflow_src_as': src_as.autonomous_system_organization})

        if dst_details:
          sflowSample.update({'sflow_dst_location': {
                               'lat': dst_details.location.latitude,
                               'lon': dst_details.location.longitude
                             },
                             'sflow_dst_country': dst_details.country.name
                            })
        if dst_as:
          sflowSample.update({'sflow_dst_as': dst_as.autonomous_system_organization})

        sflowSample.update({'sflow_NEWsrcIP': sflow_NEWsrcIP,
                            'sflow_NEWdstIP': sflow_NEWdstIP
                          })

        datestr  = time.strftime('%Y.%m.%d')
        indexstr = '%s-%s' % ('sflow', datestr)
        self.send_data.append({
                      '_index' : indexstr,
                      '_type': 'sflow',
                      '_source': sflowSample
                             })

  def tryASip(self, param):
    """
    This is a helper method which is used to lookup Autonomous System dataset for a specific IP.

    :param param: an IP
    :type param: str
    """

    try:
      details = self.ASip.asn(param)
      return details
    except geoip2.errors.AddressNotFoundError:
      return None

  def tryGeoIP(self,param):
    """
    This is a helper method which is used to lookup Geo dataset for a specific IP.

    :param param: an IP
    :type param: str
    """

    try:
      details = self.geoip.city(param)
      return details
    except geoip2.errors.AddressNotFoundError:
      return None

  def mapIP(self,param):
    """
    This is a helper method which is used to lookup a json file (with IP mappings from public to private) for a specific IP.

    :param param: an IP
    :type param: str
    """
    try:
      return test[param]
    except:
      return param

class StreamConsumer():
  """
  This class is the Kafka Consumer and it handles the streaming of messages which are read from the Kafka Cluster.

  It includes the following methods:

  * :func:`__init__`
  * :func:`runConsumer`

  """

  def __init__(self, connection, callback):
    """
    This method instantiates the Kafka Consumer with all the information that is needed.

    :param connection: It is the object that is returned when the Kafka Consumer sets the connection with the Kafka cluster.
    :type connection: KafkaConsumer
    :param callback: This is the method that handles further the message from Kafka
    :type callback: MessageHandler
    """

    self.callback   = callback
    self.connection = connection

  def runConsumer(self):
    """
    This method reads continuously from Kafka and calls the callback for each message.

    :raises: Exception - when a problem occurs possibly with the connection to Kafka cluster
    """

    try:
      for message in self.connection:
        body = message.value #json.loads(message.value)
        self.callback(body)
    except Exception, e:
      loggerConsumer.error("During messages parsing exception: %s" %e)

def newMonitor(kafkaConnections, *args):
  """
  This method is a signal handler for *SIGHUP*. The signal actually notifies when to read the config file which has some new changes in the *new_monitor* field of the *smonit* section.

  Everytime this handler runs it spawns a new process for either an existing group ID in order to scale to a new instance or a new group ID in order to add a new user of Smonnet.

  :param kafkaConnections: it includes the Kafka Consumer objects with the information about the connection to the Kafka cluster
  :type kafkaConnections: dict

  .. note::

    The right format for the *new_monitor* is:
    <action>,<filter_field>,<filter_value>,<kafka_groupId>,<eshost1>:<esport1>,<eshost2>:<esport2>,...

    Concerning the parameters:

    * action: can be used later to specify if you want to scale out or down or generally reload the whole config file
    * filter field and value: they are one of the fields of a sFlow sample e.g. source IP and the value of this e.g. 1.2.3.4
    * kafka_groupId: it's the group ID that sets another subscriber on a specific topic. This is used for new users.
    * eshost and esport: this is the cluster which Smonnet should direct the processed traffic to

  """

  config.read(config_file)
  initSet = Initialize()
  new_config = config.get('smonit', 'new_monitor').split(',')
  loggerIndex.info("New config arrived: %s" %(new_config))
  '''
  if new_config[0] == 'reload':
    new_esConnection = mon_config.ClusterConnections()
    new_filter_params = mon_config.FilterOutParams()
  else:
  '''
  new_filterParams = (new_config[1], new_config[2])
  grouId = new_config[3]
  connect2kafka = kafkaConnections.get(groupId)
  if not connect2kafka:
    connect2kafka = initSet.SetupConsumer(groupId)
    kafkaConnections.update({groupId: connect2kafka})
  monitor_nodes = config.get('smonit', 'monitor_nodes').split(',')
  new_cluster = new_config[4:]
  new_esConnection = initSet.SetupES(new_cluster)

  p = mp.Process(target=start, args=(connect2kafka,new_esConnection,new_filterParams,3))
  p.start()


def closeConsumer(connections, *args):
  """
  This is a signal handler for closing all the connections to the Kafka cluster with the SIGINT signal.

  :param connections: it includes the Kafka Consumer objects with the information about the connection to the Kafka cluster
  :type connections: dict
  """

  #TODO close a specific connection and not all
  loggerConsumer.info("Signal handler called with signal SIGINT")
  # restore the original signal handler as otherwise evil things will happen
  # in raw_input when CTRL+C is pressed, and our signal handler is not re-entrant
  original_sigint = signal.getsignal(signal.SIGINT)
  signal.signal(signal.SIGINT, original_sigint)
  loggerConsumer.debug("Close the following connections: " %(connections))
  for con in connections.values():
    con.close
  try:
    sys.exit(0)
  except KeyboardInterrupt:
    sys.exit(1)

def start(connect2kafka, esConnections, filterParams, affinity):
  """
  This method starts the whole process of consuming, processing and finally sending the Kafka messages to an Elasticsearch cluster.

  It pins every process to another core in a RoundRobin fashion, it starts consuming the messages and then calls the callback method.

  :param connect2kafka: It is the object that is returned when the Kafka Consumer sets the connection with the Kafka cluster.
  :type connect2kafka: KafkaConsumer
  :param esConnections: the Elasticsearch object with the information about the connection to the cluster
  :type esConnections: elasticsearch.Elasticsearch
  :param filterParams: it includes the sFlow field and its value that is needed to be filtered in
  :type filterParams: list
  :param affinity: this is the number for setting cpu affinity
  :type affinity: int
  """

  proc = psutil.Process()
  proc.cpu_affinity([affinity])
  handler = MessageHandler(esConnections, filterParams)
  consumer = StreamConsumer(connect2kafka, handler.Accept)
  consumer.runConsumer()

if __name__ == '__main__':
  """
  This is the method for all the iniatializations.

  It does the following:

  * sets the loggers
  * it loads the json file with the IP mappings
  * it writes the pid to the *api.cfg* conf file
  * it initializes the the Kafka connection, the filters, the Elsticsearch connections from *api.cfg*
  * it starts a new process for every different Kafka group ID
  """

  global loggerConsumer
  global loggerIndex
  global test
  kafkaConnections = {}

  config = ConfigParser.ConfigParser()
  config_file = "api.cfg"
  config.read(config_file)
  # set pid in the config file (for receiving later the SIGHUP)
  with open(config_file, 'w') as fd:
    config.set('smonit', 'processing_pid', getpid())
    config.write(fd)

  # create logger
  with open("sproc/jsonIPmap.json", 'r') as fd:
    #test = json.load(fd)
    test = cjson.decode(fd.read())
    #test = yaml.load(fd, Loader=Loader)

  loggerConsumer = logging.getLogger('kafka consumer')
  loggerConsumer.setLevel(logging.DEBUG)
  loggerIndex = logging.getLogger('es indexing')
  loggerIndex.setLevel(logging.DEBUG)

  # create console handler and set level to debug
  logDest = logging.StreamHandler()

  # create formatter
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

  # add formatter to ch
  logDest.setFormatter(formatter)

  # add ch to logger
  loggerConsumer.addHandler(logDest)
  loggerIndex.addHandler(logDest)

  mon_config = MonConfig()
  esConnections = mon_config.ClusterConnections()
  filterParams = mon_config.FilterOutParams()
  loggerIndex.info("Initialization: es connections: %s" %(esConnections))
  loggerIndex.info("Filter in params: %s" %(filterParams))

  initSet = Initialize()
  monitor_names = config.get('smonit', 'monitor_names').split('-')
  no_cores = psutil.cpu_count()
  for groupId in set(monitor_names):
    kafkaConnections.update({groupId: initSet.SetupConsumer(groupId)})

  processes =  [mp.Process(target=start,
                args=(kafkaConnections.get(monitor_names[i]),esConnections[i],filterParams[i],i%no_cores))
                for i in range(len(esConnections))]
  loggerIndex.info("Processes: %s" %(processes))
  signal.signal(signal.SIGHUP, partial(newMonitor, kafkaConnections))
  signal.signal(signal.SIGINT, partial(closeConsumer, kafkaConnections))

  for p in processes:
    p.daemon = True
    p.start()
  for p in processes:
    p.join()
