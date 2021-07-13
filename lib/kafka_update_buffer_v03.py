"""
kafka_update_buffer_v03.py
Update buffer
"""
import os, sys
import numpy as np
import pandas as pd
from EvolvingClusters import evolving_clusters_single
# from EvolvingClustersKDT import evolving_clusters_single
from helper import haversine, get_aligned_location
from kafka_config_c_p_v01 import CFG_DESIRED_ALIGNMENT_RATE_SEC, CFG_THRESHOLD_MAX_SPEED, CFG_ALIGNMENT_MODE, CFG_INIT_POINTS, CFG_EC_DISTANCE_THRESHOLD, CFG_EC_CARDINALITY_THRESHOLD, CFG_EC_TEMPORAL_THRESHOLD, CFG_PRODUCER_TIMESTAMP_NAME, CFG_PRODUCER_KEY, CFG_PRODUCER_TIMESTAMP_NAME, CFG_CONSUMER_COORDINATE_NAMES




def update_buffer(object_pool, oid, ts, lon, lat, **kwargs):
	'''
		Update the Objects' buffer, given a new location record.

		Input
		-----
		* object_pool:	The Records for all objects (Universal Buffer)
		* oid: 			The identifier of the object
		* ts:			The timestamp of the record
		* lon, lat:		The location of the record (longitude, latitude)
		* **kwargs:		Other features to be included to the buffer

		Output
		------
		* object_pool:		The Records for all objects (Universal Buffer)
	'''

	object_buffer = object_pool.loc[object_pool[CFG_PRODUCER_KEY] == oid].copy()


	if not object_buffer.values.shape[0] >= 1: # if this record is the first point of the vessel
		print('---- Cannot inter-/extra-polate for vessel: {0} ----'.format(oid))
		print('---- This is the first record for this vessel ----')
		
		new_row = {CFG_PRODUCER_KEY: oid, 
					CFG_PRODUCER_TIMESTAMP_NAME: ts, 
				   	CFG_CONSUMER_COORDINATE_NAMES[0]: lon, 
					CFG_CONSUMER_COORDINATE_NAMES[1]: lat, 
					**kwargs}
		object_pool = object_pool.append(new_row, ignore_index=True)  # append row to the dataframe
	
	else:	
		object_latest_record = object_buffer.loc[object_buffer[CFG_PRODUCER_TIMESTAMP_NAME] == object_buffer[CFG_PRODUCER_TIMESTAMP_NAME].max()]	# The latest record for mmsi: %oid
		dt = ts - object_latest_record[CFG_PRODUCER_TIMESTAMP_NAME].values[0]				# Calculate the dt of the incoming record from the one of the latest

		if dt < 1: # if this record is less than 1 sec from the previous
			print('---- Invalid record: dt < 1sec ----')
			if dt < 0: # if this record is less than 0 sec
				raise Exception('---- Records for each vessel must be sorted by timestamp ----')
		
		else:
			print('Record Timedelta: {0}sec'.format(dt))
			
			if dt > 2*CFG_DESIRED_ALIGNMENT_RATE_SEC:
				print('---- Cannot inter-/extra-polate for vessel: {0} ----'.format(oid))
			
				object_pool = object_pool.drop(object_pool.loc[object_pool[CFG_PRODUCER_KEY] == oid].index)
				new_row = {CFG_PRODUCER_KEY: oid, 
							CFG_PRODUCER_TIMESTAMP_NAME: ts, 
							CFG_CONSUMER_COORDINATE_NAMES[0]: lon, 
							CFG_CONSUMER_COORDINATE_NAMES[1]: lat, 
							**kwargs}
				
				object_pool = object_pool.append(new_row, ignore_index=True)  # append row to the dataframe
				print('---- Delete all the previous records: dt > {0}sec and id: {1} ----'.format(2*CFG_DESIRED_ALIGNMENT_RATE_SEC, oid))
			
			else:
				dist_m = haversine((lon, lat), (object_latest_record[CFG_CONSUMER_COORDINATE_NAMES[0]].values[0], object_latest_record[CFG_CONSUMER_COORDINATE_NAMES[1]].values[0])) * 10**3 # Calculate Haversine Disance (in meters)
				oid_point_speed = dist_m / dt		# Calculate (Monentrary) Object Speed
				
				if oid_point_speed > CFG_THRESHOLD_MAX_SPEED:
					print('---- Cannot inter-/extra-polate for vessel: {0} ----'.format(oid))
					print('---- Invalid speed record ({0} m/s) ----'.format(oid_point_speed))

				else:
					new_row = {CFG_PRODUCER_KEY: oid, 
								CFG_PRODUCER_TIMESTAMP_NAME: ts, 
								CFG_CONSUMER_COORDINATE_NAMES[0]: lon, 
								CFG_CONSUMER_COORDINATE_NAMES[1]: lat, 
								**kwargs}
					object_pool = object_pool.append(new_row, ignore_index=True)  	# append row to the ```objects_pool``` dataframe						

	return object_pool




def discover_evolving_clusters(timeslice, stream_active_patterns, stream_closed_patterns, coordinate_names=[CFG_CONSUMER_COORDINATE_NAMES[0], CFG_CONSUMER_COORDINATE_NAMES[1]], temporal_name=CFG_PRODUCER_TIMESTAMP_NAME, temporal_unit='s', o_id_name=CFG_PRODUCER_KEY, verbose=True):
	if verbose:
		print ('\t\t\t---- Timeslice Created ----')
		print (timeslice)
		print ('\t\t\t---- (Previous) Active Evolving Clusters ----')
		print (stream_active_patterns)
		print ('\t\t\t---- (Previous) Closed Evolving Clusters ----')
		print (stream_closed_patterns)

	if not timeslice.empty:
		print ('\t\t\t---- Discovering Evolving Clusters ----')
		stream_active_patterns, stream_closed_patterns = evolving_clusters_single(timeslice, coordinate_names=coordinate_names, temporal_name=temporal_name, temporal_unit=temporal_unit, o_id_name=o_id_name, distance_threshold=CFG_EC_DISTANCE_THRESHOLD, min_cardinality=CFG_EC_CARDINALITY_THRESHOLD, time_threshold=CFG_EC_TEMPORAL_THRESHOLD, active_patterns=stream_active_patterns, closed_patterns=stream_closed_patterns)		
	else:
		print ('\t\t\t---- Cannot Discover Evolving Clusters ----')

	if verbose:
		print ('\t\t\t---- (Current) Active Evolving Clusters ----')
		print (stream_active_patterns)
		print ('\t\t\t---- (Current) Closed Evolving Clusters ----')
		print (stream_closed_patterns)

	return stream_active_patterns, stream_closed_patterns
