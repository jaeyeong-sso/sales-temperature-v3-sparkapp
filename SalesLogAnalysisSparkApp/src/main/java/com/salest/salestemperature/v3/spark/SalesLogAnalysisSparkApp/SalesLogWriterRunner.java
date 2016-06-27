package com.salest.salestemperature.v3.spark.SalesLogAnalysisSparkApp;

import org.apache.log4j.Logger;

public class SalesLogWriterRunner implements Runnable {
	
	private static Logger logger = Logger.getLogger(SalesLogWriterRunner.class);
	
	
	public void run() {

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.info("2016-06-25-01,01:39:00,1,1,2500");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.info("2016-06-25-02,01:39:01,1,1,2500");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.info("2016-06-25-03,01:40:00,28,1,3500");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.info("2016-06-25-04,01:40:01,28,1,3500");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.info("2016-06-25-05,01:41:00,1,1,2500");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info("2016-06-25-06,01:41:00,1,1,2500");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info("2016-06-26-07,01:41:00,1,1,2500");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.info("2016-06-26-08,01:41:00,1,1,2500");
		
	}
}
