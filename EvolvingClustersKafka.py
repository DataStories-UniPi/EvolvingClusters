"""
kafka_multiprocess_v04.py
"""
import sys, os
import csv, json
import pandas as pd
import numpy as np

import pyproj
import datetime
import collections
from time import sleep
from tqdm import tqdm

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

# IMPORT SCRIPT HELPER FUNCTIONS & CONFIGURATION PARAMETERS
sys.path.append(os.path.join(os.path.dirname(__file__), 'lib'))
from helper import get_rounded_timestamp, get_aligned_location, adjust_buffers, data_output
from kafka_update_buffer_v03 import update_buffer, discover_evolving_clusters

from kafka_config_c_p_v01 import CFG_KAFKA_BIN_FOLDER, CFG_KAFKA_CFG_FOLDER, CFG_READ_FILE, CFG_WRITE_FILE, CFG_TOPIC_NAME, CFG_EC_RESULTS_TOPIC_NAME, CFG_ALIGNMENT_RESULTS_TOPIC_NAME, CFG_TOPIC_PARTITIONS, CFG_NUM_CONSUMERS, CFG_CONSUMERS_EQUAL_TO_PARTITIONS, CFG_BUFFER_COLUMN_NAMES, CFG_DESIRED_ALIGNMENT_RATE_SEC, CFG_ALIGNMENT_MODE, CFG_SAVE_TO_TOPIC, CFG_TOPIC_FF, CFG_DATASET_NAME, CFG_PRODUCER_DTYPE, CFG_PRODUCER_KEY, CFG_PRODUCER_TIMESTAMP_NAME, CFG_PRODUCER_TIMESTAMP_UNIT, CFG_CONSUMER_COORDINATE_NAMES, CFG_BUFFER_OTHER_FEATURES, CFG_EC_CARDINALITY_THRESHOLD, CFG_EC_TEMPORAL_THRESHOLD, CFG_EC_DISTANCE_THRESHOLD


# PARALLELIZING MODULES
# Consider importing Ray (https://github.com/ray-project/ray) for Process Parallelization
import subprocess
import threading, logging, time
import multiprocessing



def LoadingBar(time_sec, desc):
	for _ in tqdm(range(time_sec), desc=desc):
		sleep(1)
	return None



def wait(dt, threshold=0.1):
	if len(dt) == 1:
		''' If we're at the first record (Start of Time) '''
		sleep(0.1)
	else:
		''' If the records are coming at the same time, wait for ```threshold``` seconds '''
		sleep(max(threshold, np.diff(dt).sum()/CFG_TOPIC_FF))



def StartServer():
	"""
		Start Server
	"""
	# Single SYS Call (V2)
	os.system("{0}/zookeeper-server-start.sh {1}/zookeeper.properties & {0}/kafka-server-start.sh {1}/server.properties".format(CFG_KAFKA_BIN_FOLDER, CFG_KAFKA_CFG_FOLDER))



def KafkaTopics(topic_name):
	def fun_delete_kafka_topic(name):
		"""
			Delete Previous Kafka Topic
		"""
		client = KafkaAdminClient(bootstrap_servers="localhost:9092")
		
		print('Deleting Previous Kafka Topic(s) ...')
		if name in client.list_topics():
			print('Topic {0} Already Exists... Deleting...'.format(name))
			client.delete_topics([name])  # Delete kafka topic
		
		print("List of Topics: {0}".format(client.list_topics()))  # See list of topics


	def fun_create_topic(name):
		"""
			Create Topic
		"""
		print('Create Kafka Topic {0}...'.format(name))
		client = KafkaAdminClient(bootstrap_servers="localhost:9092")
		topic_list = []
		
		print('Create Topic with {0} Partitions and replication_factor=1'.format(CFG_TOPIC_PARTITIONS))
		topic_list.append(NewTopic(name=name, num_partitions=CFG_TOPIC_PARTITIONS, replication_factor=1))
		client.create_topics(new_topics=topic_list, validate_only=False)
		
		print("List of Topics: {0}".format(client.list_topics()))  # See list of topics
		print("Topic {0} Description:".format(name))
		print(client.describe_topics([name]))


	LoadingBar(10, desc="Creating/Cleaning Kafka Topics...")
	fun_delete_kafka_topic(topic_name)
	fun_create_topic(topic_name)



def KProducer():
	"""
		Start Producer
	"""

	LoadingBar(40, desc="Starting Kafka (Input) Producer ...")
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
	
	# TimeDelta Queue Definition
	dt = collections.deque(maxlen=2)
	# Pandas CSV File Iterator Definition
	pandas_iter = pd.read_csv(CFG_READ_FILE, iterator=True, chunksize=1, dtype=CFG_PRODUCER_DTYPE)

	for row in pandas_iter:
		row.loc[:, CFG_PRODUCER_TIMESTAMP_NAME] = row[CFG_PRODUCER_TIMESTAMP_NAME].apply(lambda x: pd.to_datetime(x, unit=CFG_PRODUCER_TIMESTAMP_UNIT).timestamp())
		
		key_mmsi = row[CFG_PRODUCER_KEY].to_json(orient='records', lines=True).encode('utf-8')
		data = row.reset_index().to_json(orient='records', lines=True).encode('utf-8')
		# print(data)
		
		timestamp_s = row[CFG_PRODUCER_TIMESTAMP_NAME].values[0]
		dt.append(timestamp_s)
		
		wait(dt)
		producer.send(CFG_TOPIC_NAME, key=key_mmsi, value=data, timestamp_ms=int(timestamp_s*10**3)) # send each csv row to consumer
		
	print('\t\t\t---- Successfully Sent Data to Kafka Topic ----')
	


def KConsumer(consumer_num, CFG_TOPIC_PARTITIONS):
	"""
		Start Consumer
	"""
	LoadingBar(15, desc="Starting Kafka Consumer ...")


	# ========================================	INITIALIZING AUXILIARY FILES	========================================
	if CFG_NUM_CONSUMERS == "None" or consumer_num == 0:
		if os.path.isfile(CFG_WRITE_FILE):
			print(CFG_WRITE_FILE, 'File Already Exists... Deleting...')
			os.remove(CFG_WRITE_FILE)

	
	# ========================================	INSTANTIATE A KAFKA CONSUMER	========================================
	if CFG_NUM_CONSUMERS == "None" or CFG_CONSUMERS_EQUAL_TO_PARTITIONS == 'no' or CFG_TOPIC_PARTITIONS != CFG_NUM_CONSUMERS:
		"""Consumer - Reads from all topics"""
		consumer = KafkaConsumer(CFG_TOPIC_NAME, bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
	
	elif CFG_CONSUMERS_EQUAL_TO_PARTITIONS == 'yes' and CFG_TOPIC_PARTITIONS == CFG_NUM_CONSUMERS:
		"""Consumer k reads from the k partition - Assign each k consumer to the k partition """
		consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
		consumer.assign([TopicPartition(topic=CFG_TOPIC_NAME, partition=consumer_num)])
	
	else:
		print('Check Configuration Parameters for #Consumers')

	
	# ========================================	INSTANTIATE A KAFKA PRODUCER (FOR DATA OUTPUT)	========================================
	if CFG_SAVE_TO_TOPIC:
		savedata_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
	else:
		savedata_producer = None

	# ========================================	NOW THE FUN BEGINS	========================================
	with open(CFG_WRITE_FILE, 'a') as fw2:
		fwriter = csv.writer(fw2)
		fwriter.writerow(['ts', 'message'])
		print('CSV File Writer Initialized...')

		# 0.	INITIALIZE THE BUFFERS AND PENDING TIMESTAMP
		object_pool = pd.DataFrame(columns=CFG_BUFFER_COLUMN_NAMES)	# create dataframe which keeps all the messages
		timeslice = pd.DataFrame(columns=CFG_BUFFER_COLUMN_NAMES)
		pending_time = None
				
		stream_active_patterns = [pd.DataFrame(), pd.DataFrame()]
		stream_closed_patterns = [pd.DataFrame(), pd.DataFrame()]

		# 0.5. 	INITIALIZE TEMPORAL PROBE (FOR MESSAGE CONSUMPTION TIME)
		msg_consume_times = []
		
		curr_offset = 0

		# 1.	LISTEN TO DATASTREAM
		for message in consumer:
			print('Incoming Message')
			print ("c{0}:t{1}:p{2}:o{3}: key={4} value={5}".format(consumer_num, message.topic, message.partition, message.offset, message.key, message.value))
			

			msg = json.loads(message.value.decode('utf-8'))
			fwriter.writerow([message.timestamp, msg])
			
			'''
				* Get the Current Datapoint's Timestamp
				* Get the Pending Timestamp (if not initialized)
			'''
			curr_time = message.timestamp	# Kafka Message Timestamp is in MilliSeconds 
			curr_pending_time = get_rounded_timestamp(curr_time, base=CFG_DESIRED_ALIGNMENT_RATE_SEC, mode=CFG_ALIGNMENT_MODE, unit='ms') 			
				
			if pending_time is None:
				pending_time = curr_pending_time
			
			print ("\nCurrent Timestamp: {0} ({1})\n".format(curr_time, pd.to_datetime(curr_time, unit='ms')))		
			print ('\nPending Timestamp: {0} ({1})\n'.format(pending_time, pd.to_datetime(pending_time, unit='s')))
			# print ('\nNext Pending Timestamp: {0} ({1})\n'.format(curr_pending_time, pd.to_datetime(curr_pending_time, unit='s')))	# For Debugging

			'''
			If the time is right:
				* Discover evolving clusters up to ```curr_time```
				* Save (or Append) the timeslice to the ```kafka_aligned_data_*.csv``` file
				* Save the Discovered Evolving Clusters 
			'''
			if pending_time < curr_pending_time:
				# Create the Timeslice
				timeslice = object_pool.groupby(CFG_PRODUCER_KEY, group_keys=False).apply(lambda l: get_aligned_location(l, pending_time, temporal_name=CFG_PRODUCER_TIMESTAMP_NAME, temporal_unit=CFG_PRODUCER_TIMESTAMP_UNIT, mode=CFG_ALIGNMENT_MODE))
				
				# Discover Evolving Clusters
				stream_active_patterns, stream_closed_patterns = discover_evolving_clusters(timeslice, stream_active_patterns, stream_closed_patterns, coordinate_names=CFG_CONSUMER_COORDINATE_NAMES, temporal_name=CFG_PRODUCER_TIMESTAMP_NAME, temporal_unit='s', o_id_name=CFG_PRODUCER_KEY, verbose=True)	
				
				# Checkpoint: Data Output
				data_output(savedata_producer, pending_time, timeslice, stream_active_patterns, stream_closed_patterns)
				
				# Adujst Buffers and Pending Timestamp
				object_pool, pending_time, timeslice = adjust_buffers(curr_pending_time, pending_time, object_pool.copy(), CFG_PRODUCER_TIMESTAMP_NAME)
			
			'''
			In any case, Update the Objects' Buffer 
			'''
			
			oid, ts, lon, lat = msg[CFG_PRODUCER_KEY], msg[CFG_PRODUCER_TIMESTAMP_NAME], msg[CFG_CONSUMER_COORDINATE_NAMES[0]], msg[CFG_CONSUMER_COORDINATE_NAMES[1]] # parameters for function update_buffer must be int/float

			object_pool = update_buffer(object_pool, oid, ts, lon, lat, **{k:msg[k] for k in CFG_BUFFER_OTHER_FEATURES})

			

def main():
	# StartServer() # start Zookeeper & Kafka
	# KafkaTopics() # Delete previous topic & Create new

	print('Start %d Consumers & 1 Producer with %d partitions' % (CFG_NUM_CONSUMERS, CFG_TOPIC_PARTITIONS))

	jobs = []

	job = multiprocessing.Process(target=StartServer) # 	Job #0: Start Kafka & Zookeeper
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_TOPIC_NAME,)) # 	Job #1: Delete previous kafka topic & Create new one (Simulating a DataStream via a CSV file)
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_EC_RESULTS_TOPIC_NAME,)) # 	Job #2: Delete previous kafka topic & Create new one (EvolvingClusters Results Output Topic)
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_ALIGNMENT_RESULTS_TOPIC_NAME,)) # 	Job #3: Delete previous kafka topic & Create new one (Alignment Results Output Topic)
	jobs.append(job)

	for i in range(CFG_NUM_CONSUMERS): # Create different consumer jobs
		job = multiprocessing.Process(target=KConsumer, args=(i,CFG_TOPIC_PARTITIONS))
		jobs.append(job)

	job = multiprocessing.Process(target=KProducer) # 	Job #4: Start Producer
	jobs.append(job)


	for job in jobs: # 	Start the Threads
		job.start()

	for job in jobs: # 	Join the Threads
		job.join()

	print("Done!")


if __name__ == "__main__":
	logging.basicConfig(
		format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
		level=logging.INFO
	)
	main()
