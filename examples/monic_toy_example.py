import pandas as pd
import sys, os

# Add MONIC module to PATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Adjust Pandas Parameters
pd.reset_option('display.max_columns', 'max_colwidth')
pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', None)

# Import Helper codebase
import importlib
import helper, monic
importlib.reload(helper), importlib.reload(monic) 

# Import Toy Dataset
from toy_input import *


# Temporal Overlap Threshold 
t_iou = 0.5

# MONIC Threshold Values
tau_match, tau_split = 0.3, 0.1

# MONIC External Cluster Evolution Categories
categories = ['emerged', 'survived', 'absorbed', 'split', 'disappeared']
evolutions = []

# Simulate Timeslice-per-Timeslice Stream (EC Online Output)
for idx, (prev_hist, curr_hist) in enumerate(zip(active_toy_clusters_paper_act, active_toy_clusters_paper_act[1:])):
    evolutions.append(monic.monic(prev_hist, curr_hist, tau_match, tau_split, t_iou))

# Merge MONIC Output
evolutions = pd.concat(evolutions, ignore_index=True)

# Re-arrange values (for consistency)
evolutions.transition = pd.Categorical(evolutions.transition, categories=categories, ordered=True)
evolutions.sort_values(['timestamp', 'transition'], inplace=True)

# Save to CSV (or to Data Stream Topic)
print(evolutions)
evolutions.to_csv('./out/monic_toy_out_act.csv', header=True, index=False)