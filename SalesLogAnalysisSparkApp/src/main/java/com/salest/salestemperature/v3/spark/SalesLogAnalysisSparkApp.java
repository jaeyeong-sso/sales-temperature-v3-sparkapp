package com.salest.salestemperature.v3.spark;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.State;

/**
 * SalesLogAnalysisSparkApp
 *
 */
public class SalesLogAnalysisSparkApp
{
	final static String TOPIC = "tr-events";
	final static String MESSAGE_BROKER = "salest-master-server:9092";
	final static String ZOOKEEPER_ENSEMBLE = "salest-master-server:2181";
	
	final static String HDFS_CHECKPOINT_DIR = "hdfs://namenode:9000/salest/spark_checkpoint";
	
    final String regEx= "[^0-9a-zA-Z-:,]+";
    final static Pattern pattern = Pattern.compile("[0-9,:-]+");
    
    private static SparkConf sparkConf;
	private static JavaSparkContext jctx;
	private static JavaStreamingContext jsctx;
	
	private static JavaPairRDD<String,String> menuInfoRDD;

    private static JavaPairRDD<String,Long> sumProductTotalSalesOfTodayRDD;
 
    private static SimpleDateFormat dateFormatYearMonthDay = new SimpleDateFormat("yyyy-MM-dd");
        
	public static void prepareContexts(){
	   	sparkConf = new SparkConf().setAppName("SalesLogAnalysisSparkApp")
	   			.setMaster("yarn-client");
	   			//.setMaster("local[2]");
	   			//.setMaster("spark://salest-master-server:7077");		//.setMaster("yarn-client");
		jctx = prepareContext(sparkConf);
		jsctx = prepareStreamingContext(jctx, 5);
		jsctx.checkpoint(HDFS_CHECKPOINT_DIR);
	}
	
   	private static JavaSparkContext prepareContext(SparkConf sparkConf){
		return new JavaSparkContext(sparkConf);
	}
	
	private static JavaStreamingContext prepareStreamingContext(JavaSparkContext jctx, long batchInterval){
	    return new JavaStreamingContext(jctx, Durations.seconds(batchInterval));
	}
	
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Inner Class / Function
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
    private static class UpdateProductRunningSum implements Function2<List<Long>,Optional<Long>,Optional<Long>> {

		public Optional<Long> call(List<Long> items, Optional<Long> current) throws Exception {
			// TODO Auto-generated method stub
			long agg = current.or(0L);
			for(Long item : items ){
				agg += item;
			}
			return Optional.of(agg);
		}
    }
        
    // Update the cumulative count function

    private static final Function4<Time, String, Optional<Long>, State<Long>, Optional<Tuple2<String, Long>>> mappingFunc =
        new Function4<Time, String, Optional<Long>, State<Long>, Optional<Tuple2<String, Long>>>() {

			public Optional<Tuple2<String, Long>> call(Time arg0, String key, Optional<Long> current, State<Long> state) throws Exception {
				// TODO Auto-generated method stub
				
				Long sum = current.or(0L) + (state.exists() ? state.get() : 0L);
			
				if(!state.isTimingOut()){
					state.update(sum);
				} else {
					sum = current.or(0L);
				}
				
				Tuple2<String, Long> output = new Tuple2<String, Long>(key,sum);

				return Optional.of(output);
			}
	};
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Functions
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	private static JavaPairRDD<String,String> loadMenuInfoRDD(JavaSparkContext jctx){

		JavaRDD<String> inputRDD = jctx.textFile("hdfs://namenode:9000/salest/menu_data/menu_code_out.csv");
		
		JavaPairRDD<String,String> productCodeCategoryPairRDD = inputRDD.mapToPair(new PairFunction<String,String,String>(){
			public Tuple2<String, String> call(String line) throws Exception {
				// TODO Auto-generated method stub
				String[] fields = line.split(",");
				return new Tuple2<String, String>(fields[1], fields[0]);
			}
		});

		return productCodeCategoryPairRDD;
	}
	
	private static JavaPairDStream<String, String> createKafkaDirectStream(JavaStreamingContext jsctx){
		
	    Set<String> topicsSet = new HashSet<String>(Arrays.asList(TOPIC));
	    Map<String, String> kafkaParams = new HashMap<String, String>();
	    kafkaParams.put("metadata.broker.list", MESSAGE_BROKER);
	    
	    return KafkaUtils.createDirectStream(
	    	jsctx,
	        String.class,
	        String.class,
	        StringDecoder.class,
	        StringDecoder.class,
	        kafkaParams,
	        topicsSet
	    );
	}
	
	private static void extractVaildSalesLogMessageDStream (JavaPairDStream<String, String> pairDStream, final JavaPairRDD<String,String> menuInfoRDD){
 
		// Return Pair("MsgId","ValidLogMessage")
	    JavaPairDStream<String,String> messagesDStream = pairDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
	        public Tuple2<String, String> call(Tuple2<String, String> tuple) {
	        	
	        	//System.out.println("Expected [MsgId,ValidLogMessage]: " + tuple._2);
	        	
	        	String[] columes = tuple._2.split("[^0-9a-zA-Z-:,]+");

	        	String msgUUID = columes[columes.length-2];
	        	String salesRecord = "";
	        	
	    		Matcher matcher = pattern.matcher(columes[columes.length-1]);
	    		if(matcher.find()){
	    			salesRecord = matcher.group(0);
	    		}
	    		//System.out.println("[msgUUID]: " + msgUUID + "     [salesRecord]: " + salesRecord);
				return new Tuple2<String, String>(msgUUID, salesRecord);
	        }
	    }).reduceByKey(new Function2<String, String, String>() {
	    	public String call(String firstSalesRecord, String secondSalesRecord) {
	    		return firstSalesRecord;
	    	}
	    });
	    

	    JavaPairDStream<String,String> productCodeOthersDStream = messagesDStream.mapToPair(new PairFunction<Tuple2<String, String>,String,String>(){
			public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, String>(SalesLogRecord.parseProductCodeFromRecord(tuple._2),tuple._2);
			}
		});
	    
	    productCodeOthersDStream.foreachRDD(new Function<JavaPairRDD<String,String>, Void>(){
	    	public Void call(JavaPairRDD<String,String> rddPair) throws Exception {

	    		JavaPairRDD<String,Tuple2<String,String>> menuInfoWithCate = menuInfoRDD.join(rddPair);
	    		JavaPairRDD<Tuple2<String,String>,Long> totalAmountByCategory =  reduceByProductCodeTotalAmountPair(menuInfoWithCate);

	    		totalAmountByCategory.foreachPartition(new VoidFunction<Iterator<Tuple2<Tuple2<String,String>,Long>>>(){
					public void call(Iterator<Tuple2<Tuple2<String,String>, Long>> iter) throws Exception {
						// TODO Auto-generated method stub
						while(iter.hasNext()){
							Tuple2<Tuple2<String,String>,Long> cateTotalAmount = iter.next();
							
						    RedisClient redisClient = new RedisClient();
						    redisClient.initialize();
						    
						    String rediskey = RedisClient.KEY_PREFIX_SALESLOG_TOTALAMOUNT + RedisClient.KEY_MIDFIX_CATEGORY
						    		+ cateTotalAmount._1._1 + ":" + cateTotalAmount._1._2;
						    
						    String readValue = redisClient.readValueByKey(rediskey);

						    redisClient.createOrIncrLongValue(rediskey, cateTotalAmount._2);
							 
							readValue = redisClient.readValueByKey(rediskey);

							//System.out.println("[SalesCount By Category] : " + rediskey + " , " + readValue);
							
							redisClient.uninitialize();
						}
					}    			
	    		});
		
				return null;
	    	}
	    });


	    JavaMapWithStateDStream<String,Long,Long,Tuple2<String, Long>> stateDStream = updateTotalAmountOfTodayByProduct(messagesDStream);

	    stateDStream.foreachRDD(new Function<JavaRDD<Tuple2<String,Long>>,Void>(){
			public Void call(JavaRDD<Tuple2<String, Long>> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>(){
					public void call(Iterator<Tuple2<String, Long>> iter) throws Exception {
						// TODO Auto-generated method stub
						while(iter.hasNext()){
							Tuple2<String,Long> tuple = iter.next();

							RedisClient redisClient = new RedisClient();
						    redisClient.initialize();
						    
						    String rediskey = RedisClient.KEY_PREFIX_SALESLOG_TOTALAMOUNT + RedisClient.KEY_MIDFIX_PRODUCT + tuple._1;
						    redisClient.createOrUpdateValueByKey(rediskey, String.valueOf(tuple._2));

							String readValue = redisClient.readValueByKey(rediskey);

							//System.out.println("[SalesCount By Product] : " + rediskey + " , " + readValue);
							
							redisClient.uninitialize();
						}
					}
				});
				return null;
			}
	    });

	    
	    //stateDstream.print();
	}
	
	
	private static JavaPairRDD<Tuple2<String,String>,Long> reduceByProductCodeTotalAmountPair(JavaPairRDD<String,Tuple2<String,String>> inputRDD){
		
		return inputRDD.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,String>>,Tuple2<String,String>,Long>(){
			public Tuple2<Tuple2<String,String>,Long> call(Tuple2<String, Tuple2<String, String>> productCodeMsgCate) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Tuple2<String,String>,Long>(
						new Tuple2<String,String>(SalesLogRecord.parseTrDateFromRecord(productCodeMsgCate._2._2), productCodeMsgCate._2._1),
						Long.parseLong(SalesLogRecord.parseSalesAmountFromRecord(productCodeMsgCate._2._2)));
			}
		}).reduceByKey(new Function2<Long, Long, Long>(){
			public Long call(Long first, Long second) throws Exception {
				// TODO Auto-generated method stub
				return first + second;
			}		
		});
	}
	
	public static JavaMapWithStateDStream<String,Long,Long,Tuple2<String, Long>> 
		updateTotalAmountOfTodayByProduct(JavaPairDStream<String,String> messagesDStream){
		
	    List<Tuple2<String, Long>> tuples = Arrays.asList(new Tuple2<String,Long>("0", 0L));
	    
	    sumProductTotalSalesOfTodayRDD = jsctx.sparkContext().parallelizePairs(tuples);
	    
	    return messagesDStream.mapToPair(new PairFunction<Tuple2<String,String>, String,Long>(){
	    		public Tuple2<String,Long> call(Tuple2<String,String> tuple) throws Exception {
	    			// TODO Auto-generated method stub
	    			String trDate = SalesLogRecord.parseTrDateFromRecord(tuple._2);
	    			String productCode = SalesLogRecord.parseProductCodeFromRecord(tuple._2);
	    			Long salesAmount = Long.parseLong(SalesLogRecord.parseSalesAmountFromRecord(tuple._2));
	    			return new Tuple2(trDate + ":" + productCode, salesAmount);
	    		}
	    	}).mapWithState(StateSpec.function(mappingFunc).timeout(Durations.minutes(60*24)).initialState(sumProductTotalSalesOfTodayRDD));   
	}
	
	public static void processSalesLogMessages(){
		
		menuInfoRDD = loadMenuInfoRDD(jctx);
		menuInfoRDD.persist(StorageLevel.MEMORY_AND_DISK());
		
		JavaPairDStream<String, String> kafkaInputDStream = createKafkaDirectStream(jsctx);
		
		//kafkaInputDStream.print();
		extractVaildSalesLogMessageDStream(kafkaInputDStream, menuInfoRDD);
	}
	
	public static void startSparkContexts(){
		jsctx.start();
		jsctx.awaitTermination();
		
		menuInfoRDD.unpersist();
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// Windows Env workaround
		//System.setProperty("hadoop.home.dir", "c:\\\\winutil\\\\" );
		
		prepareContexts();		
		processSalesLogMessages();
	
		
	  	//ExecutorService executor = Executors.newFixedThreadPool(1);
		//executor.execute(new SalesLogWriterRunner());
		
			
		startSparkContexts();
			
	}
}
