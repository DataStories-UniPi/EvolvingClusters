import pandas as pd
import numpy as np
from haversine import haversine
import networkx as nx
from tqdm import tqdm as tqdm
import geopandas as gpd
import time, datetime


def pairs_in_radius(df, diam=1000):
	'''
	Get all pairs with distance < diam
	'''
	res = []
	for ind_i, ind_j, val_i, val_j in nparray_combinations(df):
		dist = haversine(val_i, val_j)*1000
		if (dist<diam):
			res.append((ind_i,ind_j))
	return res


def connected_edges(data, circular=True):
	'''
	Get circular (all points inside circle of diameter=diam) or density based (each pair with distance<diam)
	'''
	G = nx.Graph()
	G.add_edges_from(data)
	if circular:
		return [sorted(list(cluster)) for cluster in nx.find_cliques(G)]
	else:
		return [sorted(list(cluster)) for cluster in nx.connected_components(G)]


def nparray_combinations(arr):
	'''
	Get all combinations of points
	'''
	for i in range(arr.shape[0]):
		for j in range(i+1, arr.shape[0]):
			yield i, j, arr[i,:], arr[j,:]


def translate(sets, sdf):
	'''
	Get mmsis from clustered indexes
	'''
	return [sorted(tuple([sdf.iloc[point].mmsi for point in points])) for points in sets]


def get_clusters(timeframe, diam, circular=True):
	pairs = pairs_in_radius(timeframe[['lon', 'lat']].values, diam)
	return connected_edges(pairs, circular=circular)


def get_current_clusters(sdf, ts, diam=1000, circular=True):
	'''
	Get clusters and init them as a single flock
	'''
	present = pd.DataFrame([[tuple(val)] for (val) in translate(get_clusters(sdf, diam, circular=circular), sdf )], columns=['clusters'])
	present['st'] = present['et'] = ts
	return present


def find_links(x, past, min_cardinality, inters_lst):
    tmp = past.apply(check_interesection, args=(x, min_cardinality, inters_lst, ), axis=1)
#     print(tmp)
    if set(x.clusters) not in tmp.tolist():
        x.clusters = set(x.clusters)
        inters_lst.append(x.tolist())
		
    
def check_interesection(A, B, min_cardinality, inters_lst):
    tmp = pd.DataFrame()
    inters = set(A.clusters).intersection(set(B.clusters))
    if (len(inters)>=min_cardinality): 
        inters_lst.append([inters, A.st, B.et])
        return inters
    else:
        return 0
	

def find_gps(present, past, min_cardinality):
	active = []

	present.apply(find_links, args=(past, min_cardinality, active,), axis=1)

	active = pd.DataFrame(active, columns=present.columns)
	
# 	print(active[active.clusters.apply(lambda x: set(x) == set([205204000, 228064900, 234597000, 228797000, 259019000]))])
	
	inactive = past[past.clusters.apply(lambda x: set(x) not in active.clusters.tolist())]
	
	active.clusters = active.clusters.apply(sorted).apply(tuple)

# 	active['clst'] = active.clusters.apply(sorted).apply(tuple)
	
# 	inactive.clusters = inactive.clusters.apply(sorted).apply(tuple)

	filtered_dups = active[active.duplicated('clusters', keep=False)].groupby('clusters', group_keys=False, as_index=False).apply(lambda x: pd.Series([x.clusters.unique()[0], x.st.min(), x.et.max()]))
	
# 	active.drop(['clst'], axis=1, inplace=True)
	
	if filtered_dups.duplicated(keep=False).sum() != 0:
		print(active.duplicated(keep=False).sum(), 'dups before')
		print(filtered_dups.duplicated(keep=False).sum(), 'dups after')
	try:
		filtered_dups.columns = present.columns
	except:
		pass

	return pd.concat([active[~active.duplicated('clusters', keep=False)], filtered_dups]).reset_index(drop=True), inactive.reset_index(drop=True)


def evolving_clusters(df, mode, distance_threshold=3704, min_cardinality=10, time_threshold=30, disable_progress_bar=True):
	'''
	Input
	-----
	df: The dataframe containing columns lat, lon and datetime
	mode: flocks, convoys
	distance_threshold = max distance between a pair of grouped points
	min_cardinality = min number of objects in a moving pattern
	time_threshold = minimum number of minutes for which a patterns needs to exist in order to be qualified as an evolving cluster
	disable_progress_bar = if True shows a tqdm progress bar
    
	Output
	------
	A dataframe containing every evolving cluster, its starting and ending time.
	'''
	if not {'lon', 'lat', 'datetime'}.issubset(df.columns):
		raise AttributeError('Pandas DataFrame must include "lat", "lon" and "datetime" columns')
    
	if df['datetime'].dtype == 'O':
		df['datetime'] = df.datetime.apply(lambda date_time_str: datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S'))

	active = pd.DataFrame()
	closed_patterns = pd.DataFrame()


	for ind, (ts, sdf) in tqdm(enumerate(df.groupby('datetime')), total=df.datetime.nunique(), disable=disable_progress_bar):

		if mode == 'flocks' or mode == 'f':
			present = get_current_clusters(sdf, ts, distance_threshold, circular=True)
		elif mode == 'convoys' or mode == 'c':
			present = get_current_clusters(sdf, ts, distance_threshold, circular=False)

		present = present.loc[present.clusters.apply(len)>=min_cardinality]
		if active.empty:
			active = present
			continue


		if present.empty:
			closed_patterns = closed_patterns.append(active.loc[(active.et - active.st >= pd.Timedelta(minutes=time_threshold)) | (active.st == df.datetime.min())])
			active = pd.DataFrame()
			continue


		active, inactive = find_gps(present, active, min_cardinality)

		closed_patterns = closed_patterns.append(inactive.loc[(inactive.et - inactive.st >= pd.Timedelta(minutes=time_threshold)) | (inactive.st == df.datetime.min())])

# 		if not ind % 100:
# 			print('## UPDATE ###')

# 			print(f'Size of closed: {len(closed_patterns)}')
# 			print(f'Size of currently active: {len(active)}')
# 			print('#############')

	
	return pd.concat([active, closed_patterns])


