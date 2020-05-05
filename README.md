# sf-crime-statistics

1.  How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
    * By changing spark.default.parallelism we increase the number of partitions used. This would increase throughput provided we have enough support for parallelism. It might increase or decrease latency, depending on were the partitions are located (as network traffic is much slower than local traffic).
    * By changing spark.streaming.kafka.maxRatePerPartition we can change how many input records we read from kafka. If processed fast enough this causes a higher throughput and lower latency. However, we do need to make sure that we are not taking more data than we are able to process.
1.  What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
    
    The two properties that affected efficiency the most were:
    * spark.default.parallelism: Since running on a single machine the ideal value here was equal to the number of cores. However, when running in a cluster this might not be the case as network traffic is quite expensive.
    * spark.streaming.kafka.maxRatePerPartition: We should take as many records as we are able to process. Close monitoring under different workloads is needed for this.
    
    We can check the effect these properties have on efficiency by a close inspection of the project reports. In particular, by checking the processedRowsPerSecond one can determine how fast we are processing rows. It might be necessary to test the application at different workloads.
    
    In addition to tweaking these parameters it is important for efficiency to improve the spark application itself by, for example, filtering early and reducing the amount of shuffling required.