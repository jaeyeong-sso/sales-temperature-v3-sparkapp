package com.salest.salestemperature.v3.spark.SalesLogAnalysisSparkApp;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SalesLogRecord implements Serializable {

	private String trDate;
	private String trSeqNum;
	private String trTime;
	private String trProductCode;
	private String trSalesProductNum;
	private String trSalesAmount;
	private String categoryName;
	
	// 2016-06-21-01,15:05:00,4,1,3500
	final static String regEx = "([a-zA-Z]*)([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+),([0-9,:-]+),([0-9]+),([0-9]+),([0-9]+)";
	static Pattern pattern = Pattern.compile(regEx);
	
	public SalesLogRecord(String record){
		this.parseRecord(record);
	}
	
	public static String parseProductCodeFromRecord(String record){
		
		Matcher matcher = pattern.matcher(record);
		if(matcher.find()){
			return matcher.group(7);
		} else {
			return null;
		}
	}
	
	public static String parseSalesAmountFromRecord(String record){
		
		Matcher matcher = pattern.matcher(record);
		if(matcher.find()){
			return matcher.group(9);
		} else {
			return null;
		}
	}
	
	public static String parseTrDateFromRecord(String record){
		
		Matcher matcher = pattern.matcher(record);
		if(matcher.find()){
			return String.format("%s-%s-%s", matcher.group(2),matcher.group(3),matcher.group(4));
		} else {
			return null;
		}
	}
	
	private void parseRecord(String record){
		
		Matcher matcher = pattern.matcher(record);
		
		if(matcher.find()){
			this.trDate = String.format("%s-%s-%s", matcher.group(2),matcher.group(3),matcher.group(4));
			this.trSeqNum = matcher.group(5);
			this.trTime = matcher.group(6);
			this.trProductCode = matcher.group(7);
			this.trSalesProductNum = matcher.group(8);
			this.trSalesAmount = matcher.group(9);
		}
	}
	
	public void setCategoryName(String categoryName){
		this.categoryName = categoryName;
	}
	
	public String getTrDate(){return this.trDate;};
	public String getTrSeqNum(){return this.trSeqNum;};
	public String getTrTime(){return this.trTime;};
	public String getTrProductCode(){return this.trProductCode;};
	public String getTrSalesProductNum(){return this.trSalesProductNum;};
	public String getTrSalesAmount(){return this.trSalesAmount;};
	
	public String getCategoryName(){
		return this.categoryName;
	}
	
	public String toString(){
		return String.format("[trDate]:%s, [trSeqNum]:%s, [trTime]:%s, [trProductCode]:%s, [trSalesProductNum]:%s, [trSalesAmount]:%s, [categoryName]:%s",
				trDate,trSeqNum,trTime,trProductCode,trSalesProductNum,trSalesAmount,trSalesAmount,categoryName);
	}
}