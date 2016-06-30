package com.salest.salestemperature.v3.spark;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class SalesLogAnalysisSparkAppTest 
    extends TestCase
{
	
    private static SparkConf sparkConf;
	
	private static JavaSparkContext jctx;
	private static JavaStreamingContext jsctx;
	
	public void prepareContexts(){
	   	sparkConf = new SparkConf().setAppName("SalesLogAnalysisSparkApp").setMaster("local[2]");
		jctx = prepareContext(sparkConf);
		jsctx = prepareStreamingContext(jctx, 30);
	}
	
   	private JavaSparkContext prepareContext(SparkConf sparkConf){
		return new JavaSparkContext(sparkConf);
	}
	
	private JavaStreamingContext prepareStreamingContext(JavaSparkContext jctx, long batchInterval){
	    return new JavaStreamingContext(jctx, Durations.seconds(batchInterval));
	}
	
	
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public SalesLogAnalysisSparkAppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( SalesLogAnalysisSparkAppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
    	System.out.println("SalesLogAnalysisSparkAppTest - Started");
    	
    	/*
    	// Windows Env workaround
    	System.setProperty("hadoop.home.dir", "c:\\\\winutil\\\\" );
    	
    	prepareContexts();
    	
    	if(jctx==null){
    		System.out.println("jctx==null");
    	}
    	SalesLogMsgStreamProcessor processor = new SalesLogMsgStreamProcessor(jctx, jsctx);
    	
    	processor.processSalesLogMessages();
    	processor.startSparkContexts();
    	
    	ExecutorService executor;
		executor = Executors.newFixedThreadPool(1);
	    executor.execute(new SalesLogWriterRunner());
	
    	*/
    	
        assertTrue( true );
    }
    
}
