import pandas as pd
import numpy as np
from haversine import haversine, Unit
import networkx as nx
from tqdm import tqdm as tqdm
import geopandas as gpd
import time, datetime


NUMBER_OF_CLUSTER_TYPES = 2 # The number of pattern types that will be handled by EvolvingClusters (according to the output of the ```connected_edges``` method)


def pairs_in_radius(df, diam=1000):
	'''
		Get all pairs with distance < diam
	'''
	res = []
	for ind_i, ind_j, val_i, val_j in nparray_combinations(df):
		dist = haversine(val_i, val_j, unit=Unit.KILOMETERS)*1000
		if (dist<diam):
			res.append((ind_i,ind_j))
	return res


def findMCinMCS(G, C_MCS):	
	'''
		Discover Cliques from a Maximal Connected Subgraph (MCS)
	'''
	C_MC = []

	for sg in C_MCS:
		G_sub = G.subgraph(sg)
		G_sub_MC = nx.find_cliques(G_sub)
		C_MC.extend(list(G_sub_MC))

	return C_MC


def connected_edges(data):
	'''
		Get circular (all points inside circle of diameter=diam) and density based (each pair with distance<diam) clusters
	'''		
	G = nx.Graph()
	G.add_edges_from(data)

	C_MCS = [sorted(list(cluster)) for cluster in nx.connected_components(G)]
	C_MC = [sorted(list(cluster)) for cluster in findMCinMCS(G, C_MCS)]

	return [C_MCS, C_MC]


def nparray_combinations(arr):
	'''
		Get all combinations of points
	'''
	for i in range(arr.shape[0]):
		for j in range(i+1, arr.shape[0]):
			yield i, j, arr[i,:], arr[j,:]


def translate(sets, sdf, o_id):
	'''
		Get mmsis from clustered indexes
	'''
	return [sorted(tuple([sdf.iloc[point][o_id] for point in points])) for points in sets]


def get_clusters(timeframe, diam, coords):
	'''
		Discover patterns from connectivity graph
	'''
	pairs = pairs_in_radius(timeframe[coords].values, diam)
	return connected_edges(pairs)


def get_current_clusters(sdf, ts, diam=1000, coords=['lon', 'lat'], o_id='mmsi'):
	'''
		Get clusters and init them as a single pattern
	'''
	curr_clusters = []

	for clusters_tp in get_clusters(sdf, diam, coords):
		present = pd.DataFrame([[tuple(val)] for (val) in translate(clusters_tp, sdf, o_id)], columns=['clusters'])
		present['st'] = present['et'] = ts
		curr_clusters.append(present)

	return curr_clusters


def find_links(x, past, min_cardinality, inters_lst):
	tmp = past.apply(check_interesection, args=(x, min_cardinality, inters_lst, ), axis=1)
    # print(tmp)
	if set(x.clusters) not in tmp.tolist():
		x.clusters = set(x.clusters)
		inters_lst.append(x.tolist())
		
	
def check_interesection(A, B, min_cardinality, inters_lst):
	# tmp = pd.DataFrame()
	inters = set(A.clusters).intersection(set(B.clusters))
	if (len(inters) >= min_cardinality): 
		inters_lst.append([inters, A.st, B.et])
		return inters
	else:
		return 0
	

def find_gps(present, past, min_cardinality):
	active = []

	present.apply(find_links, args=(past, min_cardinality, active,), axis=1)

	active = pd.DataFrame(active, columns=present.columns)
	
	# print(active[active.clusters.apply(lambda x: set(x) == set([205204000, 228064900, 234597000, 228797000, 259019000]))])
	
	inactive = past[past.clusters.apply(lambda x: set(x) not in active.clusters.tolist())]
	
	active.clusters = active.clusters.apply(sorted).apply(tuple)

	# active['clst'] = active.clusters.apply(sorted).apply(tuple)	
	# inactive.clusters = inactive.clusters.apply(sorted).apply(tuple)

	filtered_dups = active[active.duplicated('clusters', keep=False)].groupby('clusters', group_keys=False, as_index=False).apply(lambda x: pd.Series([x.clusters.unique()[0], x.st.min(), x.et.max()]))
	
	# active.drop(['clst'], axis=1, inplace=True)
	
	if filtered_dups.duplicated(keep=False).sum() != 0:
		print(active.duplicated(keep=False).sum(), 'dups before')
		print(filtered_dups.duplicated(keep=False).sum(), 'dups after')
	try:
		filtered_dups.columns = present.columns
	except:
		pass

	return pd.concat([active[~active.duplicated('clusters', keep=False)], filtered_dups]).reset_index(drop=True), inactive.reset_index(drop=True)



def evolving_clusters_single(timeslice, coordinate_names=['lon', 'lat'], temporal_name='ts', temporal_unit='s', o_id_name='mmsi', distance_threshold=3704, min_cardinality=10, time_threshold=30, active_patterns=[pd.DataFrame(), pd.DataFrame()], closed_patterns=[pd.DataFrame(), pd.DataFrame()]):
	'''
	Evolving Clusters for a Single Timeslice.

	Input
	-----
	timeslice: The (timeslice) dataframe that contains the (temporally aligned) GPS locations at timestamp ```t```.
	coordinate_names: A list that consists of the column names that contain the locations' coordinates.
	temporal_name: The column name of the locations' temporal dimension.
	temporal_unit: The unit of the locations' temporal dimension.
	distance_threshold = The maximum allowed distance between a pair of grouped points (meters).
	min_cardinality = The minimum allowed number of objects within a moving pattern.
	time_threshold = The minimum allowed time an existing moving pattern needs to be active in order to be qualified as an evolving cluster (minutes).
	active_patterns = The DataFrame that contains the Active Patterns up to timestamp ```t```.
	closed_patterns = The DataFrame that contains the Closed Patterns up to timestamp ```t```.

	Output
	------
	Two lists of DataFrames, one for the ```Active``` and another for the ```Closed``` Patterns. Each element in the list represents the ```Active``` (```Closed```, respectively) Patterns for each distinct type (e.g. MCS, MC).
	'''

	if not set(coordinate_names + [temporal_name]).issubset(timeslice.columns):
		raise AttributeError('Timeslice DataFrame must include proper Spatial (e.g. "lat", "lon") and Temporal (e.g. "datetime") columns.')

	ts = pd.to_datetime(timeslice[temporal_name].unique()[0], unit=temporal_unit)

	current_clusters = get_current_clusters(timeslice, ts, distance_threshold, coordinate_names, o_id_name)
	
	
	new_active_patterns = []
	new_closed_patterns = []

	for active_patterns_tp, closed_patterns_tp, C_tp in zip(active_patterns, closed_patterns, current_clusters):
		C_tp = C_tp.loc[C_tp.clusters.apply(len) >= min_cardinality]
		
		if active_patterns_tp.empty:
			active_patterns_tp = C_tp

			new_active_patterns.append(active_patterns_tp)
			new_closed_patterns.append(closed_patterns_tp)
			continue

		if C_tp.empty:
			closed_patterns_tp = closed_patterns_tp.append(active_patterns_tp.loc[(active_patterns_tp.et - active_patterns_tp.st >= pd.Timedelta(minutes=time_threshold))])
			active_patterns_tp = pd.DataFrame()

			new_active_patterns.append(active_patterns_tp)
			new_closed_patterns.append(closed_patterns_tp)
			continue

		active_patterns_tp, inactive_patterns = find_gps(C_tp, active_patterns_tp, min_cardinality)
		closed_patterns_tp = closed_patterns_tp.append(inactive_patterns.loc[(inactive_patterns.et - inactive_patterns.st >= pd.Timedelta(minutes=time_threshold))])
		
		new_active_patterns.append(active_patterns_tp)
		new_closed_patterns.append(closed_patterns_tp)

	return new_active_patterns, new_closed_patterns


def evolving_clusters(df, coordinate_names=['lon', 'lat'], temporal_name='ts', temporal_unit='s', o_id_name='mmsi', distance_threshold=3704, min_cardinality=10, time_threshold=30, disable_progress_bar=False):
	'''
	Evolving Clusters for a Single DataFrame. Basically a wrapper function of the ```evolving_clusters_single``` method.

	Input
	-----
	df: The transmitted locations, organized in temporally aligned timeslices (Pandas DataFrame).
	coordinate_names: A list that consists of the column names that contain the locations' coordinates.
	temporal_name: The column name of the locations' temporal dimension.
	temporal_unit: The unit of the locations' temporal dimension.
	distance_threshold = The maximum allowed distance between a pair of grouped points (meters).
	min_cardinality = The minimum allowed number of objects within a moving pattern.
	time_threshold = The minimum allowed time an existing moving pattern needs to be active in order to be qualified as an evolving cluster (minutes).
	disable_progress_bar = Disable progress bar for a less verbose output
	
	Output
	------
	Two lists of DataFrames, one for the ```Active``` and another for the ```Closed``` Patterns. Each element in the list represents the ```Active``` (```Closed```, respectively) Patterns for each distinct type (e.g. MCS, MC).
	'''
	global NUMBER_OF_CLUSTER_TYPES

	if df[temporal_name].dtype == 'O':
		df.loc[:, temporal_name] = pd.to_datetime(df[temporal_name], unit=temporal_unit)

	active = [pd.DataFrame()]*NUMBER_OF_CLUSTER_TYPES # The active patterns list
	closed = [pd.DataFrame()]*NUMBER_OF_CLUSTER_TYPES # The closed patterns list


	for _, incoming_timeslice in tqdm(df.groupby(temporal_name), disable=disable_progress_bar):
		active, closed = evolving_clusters_single(incoming_timeslice, coordinate_names=coordinate_names, temporal_name=temporal_name, temporal_unit=temporal_unit, o_id_name=o_id_name, distance_threshold=distance_threshold, min_cardinality=min_cardinality, time_threshold=time_threshold, active_patterns=active, closed_patterns=closed)

	evolving_clusters_tp = [pd.concat([active_tp, closed_tp]) for active_tp, closed_tp in zip(active, closed)]

	return evolving_clusters_tp
