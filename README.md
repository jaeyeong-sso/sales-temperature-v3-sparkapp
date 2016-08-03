# sales-temperature-v3-sparkapp

This Spark Application conducts the below activities.</br>
[Act #1]. Calculating a proportion per each Product Category. </br>
[Act #2]. Calculating a proportion per each Product Item - just top 10 items(others would be shown as "ETC" items).</br>
[Act #3]. Aggregate the streaming data(which was occured just in today) and map/reduce by transaction time basis.</br>
</br>
<p>
[Implementation Point]</br>
- When implementing [Act #3], utilize "mapWithState" function instead of "updateStateByKey" for better performance.</br>
  [REF.] https://databricks.com/blog/2016/02/01/faster-stateful-stream-processing-in-apache-spark-streaming.html</br>
 "timeout" chain function of "mapWithState" might guarantee to remove the depreciated streaming data from our concern.</br>
- "SalesLogRecord" helper class parse the Kafka message - to extract the target field only for convenience.
- Regarding [Act #1], conduct the Join operation between RDD from HDFS file and RDD from DStream from Kafka message stream.
  To do this, JavaStreamingContext should be derived from JavaSparkContext because operation between the multiple JavaSparkContexts dose not supported(implemented in "prepareContexts" fuction).
