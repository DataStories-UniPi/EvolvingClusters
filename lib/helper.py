import geopandas as gpd
import pandas as pd
import numpy as np
import datetime

from scipy import interpolate
from collections import deque
from tqdm import tqdm
import os, json

from kafka_config_c_p_v01 import CFG_CSV_OUTPUT_DIR, CFG_BUFFER_COLUMN_NAMES, CFG_ALIGNMENT_MODE, CFG_DESIRED_ALIGNMENT_RATE_SEC, CFG_DATASET_NAME, CFG_ALIGNMENT_RESULTS_TOPIC_NAME, CFG_EC_RESULTS_TOPIC_NAME, CFG_SAVE_TO_FILE, CFG_SAVE_TO_TOPIC, CFG_EC_CARDINALITY_THRESHOLD, CFG_EC_TEMPORAL_THRESHOLD, CFG_EC_DISTANCE_THRESHOLD


def haversine(p_1, p_2):
	'''
		Return the Haversine Distance of two points in KM
	'''
	lon1, lat1, lon2, lat2 = map(np.deg2rad, [p_1[0], p_1[1], p_2[0], p_2[1]])   
	
	dlon = lon2 - lon1
	dlat = lat2 - lat1    
	a = np.power(np.sin(dlat * 0.5), 2) + np.cos(lat1) * np.cos(lat2) * np.power(np.sin(dlon * 0.5), 2)    
	
	return 2 * 6371.0088 * np.arcsin(np.sqrt(a))


def get_rounded_timestamp(timestamp, unit='ms', base=60, mode='inter'):
	timestamp_s = pd.to_datetime(timestamp, unit=unit).timestamp()

	if mode=='inter':
		return int(base * np.floor(np.float(timestamp_s)/base))
	elif mode=='extra':
		return int(base * np.ceil(np.float(timestamp_s)/base))
	else:
		raise Exception('Invalid Alignment Mode.')


def get_aligned_location(df, timestamp, temporal_name='ts', temporal_unit='s', mode='inter'):
	x = df[temporal_name].values.astype(np.int64)
	y = df.values
	
	try:
		'''
		**Special Use-Case #1**: If the **only** record on the object's buffer happens to be at 
		the pending timestamp, return the aforementioned record, regardless of the alignment mode.
		'''
		if len(df)==1 and not df.loc[df[temporal_name] == timestamp].empty:
			# Return the Resulted Record
			return df.copy()

		'''
		**Casual Use-Case**: Otherwise, inter-/extra-polate to the pending timestamp, 
		according to the alignment mode's specifications.
		'''
		if mode == 'inter':
			f = interpolate.interp1d(x, y, kind='linear', axis=0)
		elif mode == 'extra':
			f = interpolate.interp1d(x, y, kind='linear', fill_value='extrapolate', axis=0)
		else:
			raise Exception('Invalid Alignment Mode.')
	
		''' 
		Return the Resulted Record 
		'''
		return pd.DataFrame(f(timestamp).reshape(1,-1), columns=df.columns)     
	
	except ValueError:
		'''
		**Special Use-Case #2**: If no alignment is possible, return an empty DataFrame
		'''
		return df.iloc[0:0].copy()


def reinitialize_aux_variables(timestamp):
	pending_time = timestamp
	timeslice = pd.DataFrame(columns=CFG_BUFFER_COLUMN_NAMES)

	return pending_time, timeslice	


def adjust_buffers(curr_pending_time, old_pending_time, object_pool, temporal_name):
	# 2. Crop the Objects' Pool for the next cycle
	object_pool = object_pool.loc[object_pool[temporal_name].between(old_pending_time-CFG_DESIRED_ALIGNMENT_RATE_SEC, old_pending_time+CFG_DESIRED_ALIGNMENT_RATE_SEC, inclusive=True)]
	# 1. Re-initialize auxiliary variables
	new_pending_time, timeslice = reinitialize_aux_variables(curr_pending_time)

	return object_pool, new_pending_time, timeslice


def checkpoint_csv(timeslice, active_patterns, closed_patterns):
	'''
	Save Data to CSV File
	'''

	for active_tp, closed_tp, tp in zip(active_patterns, closed_patterns, ['mcs', 'mc']):
		path = os.path.join(CFG_CSV_OUTPUT_DIR, 'kafka_{0}_evolving_clusters_{1}_params_c={3}_t={4}_theta={5}_dataset_{2}.csv'.format(CFG_ALIGNMENT_MODE, tp, CFG_DATASET_NAME, CFG_EC_CARDINALITY_THRESHOLD, CFG_EC_TEMPORAL_THRESHOLD, CFG_EC_DISTANCE_THRESHOLD))
		pd.concat([active_tp, closed_tp]).to_csv(path, index=False, header=True)

	path = os.path.join(CFG_CSV_OUTPUT_DIR, 'kafka_aligned_data_{0}_dataset_{1}.csv'.format(CFG_ALIGNMENT_MODE, CFG_DATASET_NAME))
	if os.path.isfile(path):
		timeslice.to_csv(path, mode='a', index=False, header=False)
	else:
		timeslice.to_csv(path, index=False, header=True)


def checkpoint_kafka(kafka_producer, timestamp, timeslice, active_patterns, closed_patterns):
	'''
	Save Data to Kafka Topic
	'''
	# Creating Key for Kafka Messages
	key_time = json.dumps(timestamp).encode('utf-8')

	# Save Produced Timeslice to Kafka Topic ```CFG_EC_RESULTS_TOPIC_NAME```
	kafka_producer.send(CFG_ALIGNMENT_RESULTS_TOPIC_NAME, key=key_time, value=timeslice.to_json(orient='records').encode('utf-8'), timestamp_ms=timestamp)

	# Save Produced EvolvingClusters to Kafka Topic ```CFG_EC_RESULTS_TOPIC_NAME```
	data = {'mcs': {'active': active_patterns[0].to_json(orient='records'), 'closed': closed_patterns[0].to_json(orient='records')}, 
			'mc':  {'active': active_patterns[1].to_json(orient='records'), 'closed': closed_patterns[1].to_json(orient='records')}}

	kafka_producer.send(CFG_EC_RESULTS_TOPIC_NAME, key=key_time, value=json.dumps(data).encode('utf-8'), timestamp_ms=timestamp)


def data_output(kafka_producer, timestamp, timeslice, active_patterns, closed_patterns):
	if CFG_SAVE_TO_FILE:
		checkpoint_csv(timeslice, active_patterns, closed_patterns)
	if CFG_SAVE_TO_TOPIC:
		checkpoint_kafka(kafka_producer, timestamp, timeslice, active_patterns, closed_patterns)
		