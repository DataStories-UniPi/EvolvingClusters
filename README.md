# EvolvingClusters: Online Discovery of Group Patterns in Mobility Data

A Python3 implementation of the EvolvingClusters algorithm.



## Installation

Install the dependencies included with the following command
``` Python
pip install -r requirements.txt
```


## Usage

EvolvingClusters can be used in two variations, depending on the use-case. If EvolvingClusters is part of general online pipeline, the ```evolving_clusters_single``` method should be used, as the following example suggests:

```Python
import pandas as pd
from EvolvingClusters import evolving_clusters_single

df = Dataset_Pandas_DataFrame

active = [pd.DataFrame(), pd.DataFrame()] # The active patterns list
closed = [pd.DataFrame(), pd.DataFrame()] # The closed patterns list

for incoming_timeslice in df.groupby('timestamp'):
    ...
    active, closed = evolving_clusters_single(timeslice, distance_threshold=1852, min_cardinality=5, time_threshold=15, active_patterns=active, closed_patterns=closed)
    ...

evolving_clusters_tp = [pd.concat(active_tp, closed_tp) for active_tp, closed_tp in zip(active, closed)]
```

To use EvolvingClusters in standalone mode, the ```evolving_clusters``` method should be used, as suggested by the following example:

```Python
import pandas as pd
from EvolvingClusters import evolving_clusters

df = Dataset_Pandas_DataFrame

evolving_clusters_tp = evolving_clusters(df, min_cardinality=5, time_threshold=15, distance_threshold=1852)
```



## Kafka Implementation

Except (baseline) Python applications, EvolvingClusters can also be used as part of data streaming pipelines. The ```EvolvingClustersKafka.py``` script demonstrates that, by integrating the algorithm into the popular data streaming platform, [Apache Kafka](https://kafka.apache.org/).

The aforementioned script loads a CSV file into a Kafka Topic, consumes it (one message at a time), and saves the aligned timeslices as well as the discovered evolving clusters into their respective Kafka Topics, ready to be used by another application.

More regarding the operation of ```EvolvingClustersKafka.py``` can be found at ```doc/ec_kafka_tr.pdf``` and ```lib/README.md```.



## Contributors

Andreas Tritsarolis, George S. Theodoropoulos, Yannis Theodoridis; Data Science Lab., University of Piraeus



## Acknowledgement

This work was partially supported by projects i4Sea (grant T1EDK-03268) and Track&Know (grant agreement No 780754), which have received funding by the European Regional Development Fund of the EU and Greek national funds (through the Operational Program Competitiveness, Entrepreneurship and Innovation, under the call Research-Create-Innovate) and the EU Horizon 2020 R&I Programme, respectively.


## Citation info

If you use EvolvingClusters in your project, we would appreciate citations to the following paper:

> Andreas Tritsarolis, George-Stylianos Theodoropoulos, Yannis Theodoridis (2020) Online discovery of co-movement patterns in mobility data. International Journal of Geographical Information Science, Taylor & Francis. https://doi.org/10.1080/13658816.2020.1834562

