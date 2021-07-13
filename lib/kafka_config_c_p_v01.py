"""
cfg.py
--- SET PARAMETERS FOR KAFKA SERVER & CONSUMER & PRODUCER ---
"""
from numpy import array
from datetime import datetime
import os, sys

### Folder where Kafka & ZooKeeper exist
CFG_BASEPATH = os.path.dirname(__file__)
CFG_CSV_OUTPUT_DIR = os.path.join(CFG_BASEPATH, '..', 'data', 'csv')

CFG_KAFKA_FOLDER = os.path.join(CFG_BASEPATH, 'kafka_2.12-2.5.0')
CFG_KAFKA_BIN_FOLDER = os.path.join(CFG_KAFKA_FOLDER, 'bin')
CFG_KAFKA_CFG_FOLDER = os.path.join(CFG_KAFKA_FOLDER, 'config')


### CSV file which Kafka Producer reads
# ----------------------------------------------------------------------------------------------------------------------------------------------
# CFG_READ_FILE = os.path.join(CFG_CSV_OUTPUT_DIR, 'ais_brest_jan_24.csv')              # Brest Dataset (24/01/2016)
# ----------------------------------------------------------------------------------------------------------------------------------------------
CFG_READ_FILE = os.path.join(CFG_CSV_OUTPUT_DIR, 'ais_saronikos_apr_14_4hrs.csv')     # Saronikos Dataset (14/04/2018 -- 06:00-10:00 [4 hrs.])
# ----------------------------------------------------------------------------------------------------------------------------------------------


CFG_DATASET_NAME = CFG_READ_FILE.split('/')[-1].split('.')[0]


#################################################################################
############################## PRODUCER PARAMETERS ##############################
#################################################################################



### Dataset special dtypes
# -------------------------------------------------------------------------------------------
# CFG_PRODUCER_DTYPE = {'mmsi':int, 'ts':int}                           # Brest Dataset
# -------------------------------------------------------------------------------------------
CFG_PRODUCER_DTYPE = {'mmsi':int, 'timestamp':int}                    # Saronikos Dataset
# -------------------------------------------------------------------------------------------



### Dataset Essential Features
# -----------------------------------------------------------------------------------
# CFG_PRODUCER_KEY = 'mmsi'                                     # Brest Dataset
# CFG_PRODUCER_TIMESTAMP_NAME = 'ts'                            # Brest Dataset
# CFG_CONSUMER_COORDINATE_NAMES = ['lon', 'lat']                # Brest Dataset
# -----------------------------------------------------------------------------------
CFG_PRODUCER_KEY = 'mmsi'                                     # Saronikos Dataset
CFG_PRODUCER_TIMESTAMP_NAME = 'timestamp'                     # Saronikos Dataset
CFG_CONSUMER_COORDINATE_NAMES = ['lon', 'lat']                # Saronikos Dataset
# -----------------------------------------------------------------------------------


### Dataset Temporal Unit
# -------------------------------------------------------------
# CFG_PRODUCER_TIMESTAMP_UNIT = 's'       # Brest Dataset
# -------------------------------------------------------------
CFG_PRODUCER_TIMESTAMP_UNIT = 'ms'      # Saronikos Dataset
# -------------------------------------------------------------



### Kafka Output Topic(s) Suffix (useful for multiple concurrent instances -- can be ```None```)
# -------------------------------------------------------------
# CFG_TOPIC_SUFFIX = '_brest'             # Brest Dataset
# -------------------------------------------------------------
CFG_TOPIC_SUFFIX = '_saronikos'         # Saronikos Dataset
# -------------------------------------------------------------


### Topic Name which Kafka Producer writes & Kafka Consumer reads
CFG_TOPIC_NAME = 'datacsv{0}'.format(CFG_TOPIC_SUFFIX)

### Topic Data Throughput Factor (i.e. Fast-Forward)
CFG_TOPIC_FF = 32




#################################################################################
############################## CONSUMER PARAMETERS ##############################
#################################################################################

### DateTime format
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

### File where Kafka Consumer writes
CFG_WRITE_FILE = os.path.join(CFG_CSV_OUTPUT_DIR, 'MessagesKafka{0}.csv'.format(CFG_TOPIC_SUFFIX))

### Topic Name which Kafka Consumer writes the Evolving Clusters at each Temporal Instance
CFG_EC_RESULTS_TOPIC_NAME = 'ecdresults{0}'.format(CFG_TOPIC_SUFFIX)

### Topic Name which Kafka Consumer writes the Evolving Clusters at each Temporal Instance
CFG_ALIGNMENT_RESULTS_TOPIC_NAME = 'alignedata{0}'.format(CFG_TOPIC_SUFFIX)

### Topic num of partitions (must be an integer)
CFG_TOPIC_PARTITIONS = 1

### Num of consumers (must be an integer)
CFG_NUM_CONSUMERS = 1

### Num of consumers equal to Num of partitions
CFG_CONSUMERS_EQUAL_TO_PARTITIONS = 'yes' if CFG_TOPIC_PARTITIONS == CFG_NUM_CONSUMERS else 'no' #'yes' or 'no'


### Buffer/Timeslice Settings
# --------------------------------------------------------------------------------------------------------------------------------------------------------
# CFG_BUFFER_COLUMN_NAMES = ['id', 'mmsi', 'ts', 'lon', 'lat', 'speed', 'traj_id', 'trip_id']                                        # Brest Dataset
# --------------------------------------------------------------------------------------------------------------------------------------------------------
CFG_BUFFER_COLUMN_NAMES = ['timestamp', 'mmsi', 'lon', 'lat', 'heading', 'speed', 'course']                                        # Saronikos Dataset
# --------------------------------------------------------------------------------------------------------------------------------------------------------

	
### Dataset Non-essential Features
CFG_BUFFER_OTHER_FEATURES = sorted(list(set(CFG_BUFFER_COLUMN_NAMES) - (set([CFG_PRODUCER_KEY]) | set([CFG_PRODUCER_TIMESTAMP_NAME]) | set(CFG_CONSUMER_COORDINATE_NAMES))))


### Number of necessary records for temporal alignment
CFG_INIT_POINTS = 2


### Data-Point Alignment Interval (seconds)
# CFG_DESIRED_ALIGNMENT_RATE_SEC = 30     # 30 seconds = 0.5 minutes
CFG_DESIRED_ALIGNMENT_RATE_SEC = 60     # 1 minute
# CFG_DESIRED_ALIGNMENT_RATE_SEC = 120    # 2 minutes
# CFG_DESIRED_ALIGNMENT_RATE_SEC = 300    # 5 minutes
# CFG_DESIRED_ALIGNMENT_RATE_SEC = 600    # 10 minutes


### Maximum Speed Threshold (Vessels -- m/s)
# --------------------------------------------------------------------------------------------------------
CFG_THRESHOLD_MAX_SPEED = 25          # 50 knots = 92.6 km/h = 25.72 meters / second --- BREST/SARONIKOS
# --------------------------------------------------------------------------------------------------------


### EVOLVING CLUSTERS PARAMETERS
# -------------------------------------------------------
CFG_ALIGNMENT_MODE = 'extra'        # {'inter', 'extra'}
# -------------------------------------------------------
# -------------------------------------------------------
# Brest/Saronikos Default Parameters
# -------------------------------------------------------
CFG_EC_CARDINALITY_THRESHOLD = 5    # number of objects
CFG_EC_TEMPORAL_THRESHOLD = 15      # in minutes
CFG_EC_DISTANCE_THRESHOLD = 1000    # in meters
# -------------------------------------------------------


### DATA OUTPUT PARAMETERS
CFG_SAVE_TO_FILE  = True           # **True**: Output Data to CSV File
CFG_SAVE_TO_TOPIC = True           # **True**: Output Data to Kafka Topic