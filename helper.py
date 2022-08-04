import geopandas as gpd
import pandas as pd
import numpy as np
import shapely 
from tqdm import tqdm


def get_bbox_poly(bbox):
    p1 = shapely.geometry.Point(bbox[0], bbox[3])
    p2 = shapely.geometry.Point(bbox[2], bbox[3])
    p3 = shapely.geometry.Point(bbox[2], bbox[1])
    p4 = shapely.geometry.Point(bbox[0], bbox[1])

    np1 = (p1.coords.xy[0][0], p1.coords.xy[1][0])
    np2 = (p2.coords.xy[0][0], p2.coords.xy[1][0])
    np3 = (p3.coords.xy[0][0], p3.coords.xy[1][0])
    np4 = (p4.coords.xy[0][0], p4.coords.xy[1][0])

    mbb_area = gpd.GeoDataFrame(gpd.GeoSeries(shapely.geometry.Polygon([np1, np2, np3, np4])), 
                                columns=['geom'], geometry='geom', crs='epsg:4326')

    return shapely.geometry.MultiPolygon([mbb_area.iloc[0,0]])


def get_bbox(points):
    return get_bbox_poly(points.agg([np.min, np.max]).values.flatten())


def calculate_temporal_iou(pred_life, raw_life):
    min_st, max_st = min(pred_life[0], raw_life[0]), max(pred_life[0], raw_life[0])
    min_et, max_et = min(pred_life[1], raw_life[1]), max(pred_life[1], raw_life[1])
    
    nom = (min_et - max_st).total_seconds()
    denom = (max_et - min_st).total_seconds()

    return max(0, nom)/denom


def get_clusters_points(timeslices, cluster):
    points = timeslices.loc[(timeslices['mmsi(t)'].isin(cluster.clusters)) & 
                            (timeslices['datetime'].between(cluster.st, cluster.et, inclusive=True))].copy()
    return points


def calculate_spatial_iou(raw_mbb, pred_mbb):
    return raw_mbb.intersection(pred_mbb).area / (raw_mbb.union(pred_mbb).area + 10**(-9))


def calculate_mean_spatial_iou(timeslices, p_pred, p_raw, disable_bar=True):
    # print(p_pred, p_raw)
    timeslices_cropped = timeslices.loc[timeslices['datetime'].between(max(p_pred.st, p_raw.st), min(p_pred.et, p_raw.et), inclusive=True)].copy()
    
    tqdm.pandas(desc='Calculating Pred MBBs', leave=False, disable=disable_bar)
    mbbs_pred = get_clusters_points(timeslices_cropped, p_pred).groupby('t(t)').progress_apply(lambda l: get_bbox(l[['pred_WGS84lon(t)', 'pred_WGS84lat(t)']]))
    mbbs_pred.name = 'pred'
    # print(mbbs_pred)
    
    tqdm.pandas(desc='Calculating Raw MBBs', leave=False, disable=disable_bar)
    mbbs_raw  = get_clusters_points(timeslices_cropped, p_raw).groupby('t(t)').progress_apply(lambda l: get_bbox(l[['WGS84lon(t)', 'WGS84lat(t)']]))
    mbbs_raw.name = 'raw'
    # print(mbbs_raw)

    tqdm.pandas(desc='Calculating IoU', leave=False, disable=disable_bar)
    mbbs_merge = pd.merge(mbbs_pred, mbbs_raw, how='inner', on='t(t)').reset_index().progress_apply(lambda l: calculate_spatial_iou(l.pred, l.raw), axis=1)    
    
    if mbbs_merge.empty:
        return 0
    
    return mbbs_merge.mean() 


def jaccard_score(first, second):
    return len(first.intersection(second)) / len(first.union(second))


def monic_score(first, second):
    return len(first.intersection(second)) / len(first)


def cluster_similarity(mt_mcs_pred, mt_mcs_raw):
    # ## Measuring Cluster Similarity
    mt_mcs_best_matches = []

    for p_pred in tqdm(mt_mcs_pred.itertuples(), desc='Searching for Best Matches...', total=len(mt_mcs_pred), position=0):
        mt_mcs_matches = []
        
        for p_raw in mt_mcs_raw.itertuples():
            cluster_sim = monic_score(set(p_pred.clusters), set(p_raw.clusters))            
            mt_mcs_matches.append([*p_pred[1:], *p_raw[1:], cluster_sim])

        mt_mcs_matches = pd.DataFrame(mt_mcs_matches, columns=['clusters_prev', 'prev_st', 'prev_et', 'clusters_curr', 'curr_st', 'curr_et', 'overlap'])
        mt_mcs_best_matches.append(mt_mcs_matches)
        
    return pd.concat(mt_mcs_best_matches)


def collapse_parent_clusters(cluster_hist):
    pd.merge(cluster_hist, cluster_hist, how='outer', on='clusters', indicator=True)

    indices_to_drop = []

    for ci in cluster_hist.itertuples():
        for cj in cluster_hist.itertuples():
            if ci.Index == cj.Index:
                continue

            if set(ci.clusters).issubset(set(cj.clusters)):
                indices_to_drop.append(ci.Index if len(ci.clusters) < len(cj.clusters) else cj.Index)

    return cluster_hist.drop(indices_to_drop, axis=0)   


def emerged(prev, curr):
    emerged_clusts = []
    for clust in curr.itertuples():
        if clust.clusters not in prev.clusters.values.tolist():
            emerged_clusts.append(clust.clusters)
            print(f'Cluster {clust.clusters} Emerged at {clust.et}')
    # 
    return emerged_clusts


def survived(matches):
    survived = matches.loc[matches.clusters_prev == matches.clusters_curr].copy()
    matches.drop(survived.index, axis=0, inplace=True)
    #
    [print(f'Cluster {curr_clust.clusters_prev} Survived at {curr_clust.curr_et}') for curr_clust in survived.itertuples()]
    return [curr_clust.clusters_prev for curr_clust in survived.itertuples()]
      
      
def cluster_split(clust, curr_clusts, emerged_clusts, tau_split, tau_match):
    curr_clusts.drop(curr_clusts.loc[(~(curr_clusts.overlap >= tau_split)) | (~curr_clusts.clusters_curr.isin(emerged_clusts))].index, axis=0, inplace=True)
    if not len(curr_clusts) > 1:
        return []
    
    clust_union = set().union(*curr_clusts.clusters_curr.values)
    if monic_score(set(clust), clust_union) >= tau_match:
        print(f'Cluster {clust} Split to {curr_clusts.clusters_curr.values} at {curr_clusts.curr_st.max()}')
        return curr_clusts.clusters_curr.values.tolist()
