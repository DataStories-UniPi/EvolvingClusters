import pandas as pd
import numpy as np


active_toy_clusters_hist = [
    pd.DataFrame([], columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:22:00')],
                    [('d', 'e', 'f'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:22:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:23:00')],
                    [('d', 'e', 'f'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:23:00')],
                    [('a', 'b', 'c', 'g'), pd.Timestamp('2018-06-01 07:23:00'), pd.Timestamp('2018-06-01 07:23:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:24:00')],
                    [('a', 'b', 'c', 'o'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:24:00')],
                    [('a', 'b', 'c', 'p'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:24:00')],
                    [('d', 'e', 'f'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:24:00')],
                    [('h', 'i', 'j'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:24:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:25:00')],
                    [('a', 'b', 'c', 'o'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:25:00')],
                    [('a', 'b', 'c', 'p'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:25:00')],
                    [('h', 'i', 'j'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:25:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:26:00')],
                    [('a', 'b', 'c', 'o'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:26:00')],
                    [('a', 'b', 'c', 'p'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:26:00')],
                    [('h', 'i', 'j'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:26:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:27:00')],
                    [('h', 'i', 'j'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:27:00')],
                    [('a', 'b', 'c', 'h', 'i', 'j'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:27:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:28:00')],
                    [('h', 'i', 'j'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:28:00')],
                    [('a', 'b', 'c', 'h', 'i', 'j'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:28:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:29:00')],
                    [('h', 'i', 'j'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:29:00')],
                    [('a', 'b', 'c', 'h', 'i', 'j'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:29:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:30:00')],
                    [('h', 'i', 'j'), pd.Timestamp('2018-06-01 07:24:00'), pd.Timestamp('2018-06-01 07:30:00')],
                    [('a', 'b', 'c', 'h', 'i', 'j'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:30:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'h', 'i'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:31:00')], 
                    [('b', 'c', 'j'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:31:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'h', 'i'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:32:00')], 
                    [('b', 'c', 'j'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:32:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'h', 'i'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:33:00')], 
                    [('b', 'c', 'j'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:33:00')],
                    [('a', 'h', 'i', 'k', 'l'), pd.Timestamp('2018-06-01 07:33:00'), pd.Timestamp('2018-06-01 07:33:00')], 
                    [('b', 'c', 'j', 'm'), pd.Timestamp('2018-06-01 07:33:00'), pd.Timestamp('2018-06-01 07:33:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'h', 'i'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:34:00')], 
                    [('b', 'c', 'j'), pd.Timestamp('2018-06-01 07:27:00'), pd.Timestamp('2018-06-01 07:34:00')],
                    [('a', 'h', 'i', 'k', 'l'), pd.Timestamp('2018-06-01 07:33:00'), pd.Timestamp('2018-06-01 07:34:00')], 
                    [('b', 'c', 'j', 'm'), pd.Timestamp('2018-06-01 07:33:00'), pd.Timestamp('2018-06-01 07:34:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame([], columns=['clusters', 'st', 'et']),
    
    pd.DataFrame([], columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'h', 'i'), pd.Timestamp('2018-06-01 07:36:00'), pd.Timestamp('2018-06-01 07:36:00')], 
                    [('b', 'c', 'j'), pd.Timestamp('2018-06-01 07:36:00'), pd.Timestamp('2018-06-01 07:36:00')],
                    [('a', 'h', 'i', 'l'), pd.Timestamp('2018-06-01 07:36:00'), pd.Timestamp('2018-06-01 07:36:00')], 
                    [('b', 'c', 'j', 'm'), pd.Timestamp('2018-06-01 07:36:00'), pd.Timestamp('2018-06-01 07:36:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
]


active_toy_clusters_paper_act = [
    pd.DataFrame([], columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c', 'd', 'e'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:22:00')],
                    [('g', 'h', 'i'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:22:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),

    pd.DataFrame(np.array([
                    [('a', 'b', 'c', 'd', 'e'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:23:00')],
                    [('g', 'h', 'i'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:23:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),

    pd.DataFrame(np.array([
                    [('a', 'b', 'c', 'd', 'e'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:24:00')],
                    [('g', 'h', 'i'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:24:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c', 'd', 'e'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:25:00')],
                    [('g', 'h', 'i'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:25:00')],
                    [('f', 'g', 'h', 'i'), pd.Timestamp('2018-06-01 07:25:00'), pd.Timestamp('2018-06-01 07:25:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c', 'd', 'e'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:26:00')],
                    [('g', 'h', 'i'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:26:00')],
                    [('f', 'g', 'h', 'i'), pd.Timestamp('2018-06-01 07:25:00'), pd.Timestamp('2018-06-01 07:26:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
]


active_toy_clusters_paper_pred = [
    pd.DataFrame([], columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c', 'd', 'e'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:22:00')],
                    [('g', 'h', 'i'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:22:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),

    pd.DataFrame(np.array([
                    [('a', 'b', 'c', 'd', 'e'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:23:00')],
                    [('g', 'h', 'i'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:23:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),

    pd.DataFrame(np.array([
                    [('a', 'b', 'c', 'd', 'e'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:24:00')],
                    [('g', 'h', 'i'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:24:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c', 'd', 'e'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:25:00')],
                    [('g', 'h', 'i'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:25:00')],
                    [('f', 'g', 'h', 'i'), pd.Timestamp('2018-06-01 07:25:00'), pd.Timestamp('2018-06-01 07:25:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
    
    pd.DataFrame(np.array([
                    [('a', 'b', 'c', 'd', 'e'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:26:00')],
                    [('g', 'h', 'i'), pd.Timestamp('2018-06-01 07:22:00'), pd.Timestamp('2018-06-01 07:26:00')],
                    [('f', 'g', 'h', 'i'), pd.Timestamp('2018-06-01 07:25:00'), pd.Timestamp('2018-06-01 07:26:00')],
                    [('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'), pd.Timestamp('2018-06-01 07:26:00'), pd.Timestamp('2018-06-01 07:26:00')],
                ], dtype=object), columns=['clusters', 'st', 'et']),
]