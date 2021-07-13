# EvolvingClusters: Online Discovery of Group Patterns in Mobility Data

Integrating the EvolvingClusters algorithm into the Apache Kafka platform.



## Installation

Install the dependencies included with the following command
``` Python
pip install -r requirements.txt
```



## File Description

  * ```kafka_config_c_p_v01.py```: Secondary script; Configuration parameters for the Main script.
  * ```kafka_update_buffer_v03.py```: Methods for creating (and maintaining) the objects' buffer, and discovering evolving clusters.
  * ```helper.py```: Auxiliary methods for temporal alignment, buffer adjustment, and data output



## Usage

Prior to Evolving Clusters discovery, the user must set the parameters of the dataset (e.g. features, primary key, output directory etc.), as well as the algorithm's (e.g. step, cardinality threshold, etc.) to the ```kafka_config_c_p_v01.py``` file. 

After setting the appropriate parameters, execute the ```EvolvingClustersKafka.py``` script, as the following example suggests:

```Python
  python EvolvingClustersKafka.py
```


## Data Output

The output of the algorithm is either at a CSV file, or a Kafka Topic, depending on the values of ```CFG_SAVE_TO_FILE```, and ```CFG_SAVE_TO_TOPIC``` parameters of ```kafka_config_c_p_v01.py```.

The output of the CSV files follow the below template:

  * Aligned timeslices:
      > <pre>kafka_aligned_data_{CFG_ALIGNMENT_MODE}_dataset_{CFG_DATASET_NAME}.csv</pre>

  * Evolving clusters:
      ><pre>kafka_{CFG_ALIGNMENT_MODE}_evolving_clusters_{"mcs", "mc"}_params_c={CFG_EC_CARDINALITY_THRESHOLD}_t={CFG_EC_TEMPORAL_THRESHOLD}_theta={CFG_EC_DISTANCE_THRESHOLD}_dataset_{CFG_DATASET_NAME}.csv</pre>

The names of the Kafka Topics follow the below template:
  * Aligned timeslices:
    > <pre>ecdresults{CFG_TOPIC_SUFFIX}</pre>

  * Evolving clusters:
    > <pre>alignedata{CFG_TOPIC_SUFFIX}</pre>

**CFG_ALIGNMENT_MODE**, **CFG_DATASET_NAME**, **CFG_EC_CARDINALITY_THRESHOLD**, **CFG_EC_TEMPORAL_THRESHOLD**, **CFG_EC_DISTANCE_THRESHOLD** and **CFG_TOPIC_SUFFIX** parameters can be set at the ```kafka_config_c_p_v01.py``` file. 



## Contributors
Andreas Tritsarolis, Yannis Theodoridis; Data Science Lab., University of Piraeus