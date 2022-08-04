import pandas as pd
import numpy as np
import helper 


MONIC_RES_COLS = ['cluster_before', 'transition', 'cluster_after', 'timestamp']


def monic(prev_hist, curr_hist, tau_match, tau_split, t_iou):
    print('\n\n')
    curr_ts = curr_hist.et.max()

    # MONIC Final Result 
    clust_evolution = []


    # Degenerate Cases
    # 1. All Empty
    if prev_hist.empty and curr_hist.empty:
        return pd.DataFrame([], columns=MONIC_RES_COLS)
    # 2. No **Current** Clusters Found (all Disappeared)
    elif curr_hist.empty:
        for clust in prev_hist.itertuples():
            print(f'Cluster {clust.clusters} Disappeared at {clust.et}')
            clust_evolution.append(pd.DataFrame([[clust.clusters, 'disappeared', np.nan, clust.et],], columns=MONIC_RES_COLS))
        return pd.concat(clust_evolution, ignore_index=True)
 

    # Check Cluster Emerge
    emerged_clusts = helper.emerged(prev_hist, curr_hist)
    clust_evolution.extend([pd.DataFrame([[np.nan, 'emerged', clust, curr_ts],], columns=MONIC_RES_COLS) for clust in emerged_clusts])


    # Degenerate Cases
    # 3. If no clusters are discovered, move on to the nexts
    if prev_hist.empty:
        return pd.concat(clust_evolution, ignore_index=True)
        

    # Calculate Cluster Similarity
    cluster_matches = helper.cluster_similarity(prev_hist, curr_hist).reset_index(drop=True)

    # Check Cluster Survival
    survived_clusts = helper.survived(cluster_matches)
    clust_evolution.extend([pd.DataFrame([[clust, 'survived', clust, curr_ts],], columns=MONIC_RES_COLS) for clust in survived_clusts])
    
    
    # Check previous clusters for potential transitions
    for (clust, indices) in cluster_matches.groupby('clusters_prev').groups.items():
        curr_clusts = cluster_matches.loc[indices].copy()       
        curr_clusts = curr_clusts.loc[curr_clusts.overlap >= tau_match].copy()
        
        # Check Cluster Disappearance
        disappeared = clust not in curr_hist.clusters.unique().tolist()
        if disappeared:
            print(f'Cluster {clust} Disappeared at {curr_ts}')
            clust_evolution.append(pd.DataFrame([[clust, 'disappeared', np.nan, curr_ts],], columns=MONIC_RES_COLS))

            # Check Cluster Split
            if not curr_clusts.empty and (curr_clusts.apply(lambda l: helper.calculate_temporal_iou((l.curr_st, l.curr_et), (l.prev_st, l.prev_et)), axis=1) >= t_iou).any():
                split_clusts = helper.cluster_split(clust, curr_clusts, emerged_clusts, tau_split, tau_match)
                
                # Add to History only if the list is non-empty
                if split_clusts != []:
                    clust_evolution.append(pd.DataFrame([[clust, 'split', split_clusts, curr_ts],], columns=MONIC_RES_COLS))
            continue


        # Check Cluster Absorption
        absorbed = []
        for curr_clust in curr_clusts.itertuples():
            others = cluster_matches.loc[(cluster_matches.overlap >= tau_match) & (cluster_matches.clusters_curr == curr_clust.clusters_curr)].copy()
            
            if len(others) >= 1 and curr_clust.clusters_curr in emerged_clusts:
                print(f'Cluster {clust} Absorbed to {curr_clust.clusters_curr} at {curr_clust.curr_et}')
                clust_evolution.append(pd.DataFrame([[clust, 'absorbed', curr_clust.clusters_curr, curr_clust.curr_et],], columns=MONIC_RES_COLS))

                absorbed.append(curr_clust.Index)        
        curr_clusts.drop(absorbed, axis=0, inplace=True)
    
    # Output Result
    return pd.concat(clust_evolution, ignore_index=True)