package com.salest.salestemperature.v3.spark.SalesLogAnalysisSparkApp;

import java.io.Serializable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisClient implements Serializable {
	
	public static String KEY_PREFIX_SALESLOG_TOTALAMOUNT_OF_CATE = "saleslog_totalamount_of:";

	private JedisPool jedisPool;
	private Jedis jedis;
	
	public void initialize(){
		this.jedisPool = new JedisPool(new JedisPoolConfig(), "salest-master-server", 6300); 
		this.jedis = this.jedisPool.getResource(); 
	}
	
	public void uninitialize(){
		if(this.jedisPool != null) {
			this.jedisPool.close();
			this.jedisPool.destroy();
		}
	}
	
	public String readValueByKey(String key){
		
		String readKey = null;

		try { 
			readKey = jedis.get(key); 
			
		} catch(JedisConnectionException e){ 
			if(null != jedis){ 
				readKey = null;
			} 
		} finally { 
			//jedisPool.close();
		} 
		return readKey;
	}
	
	public void createOrUpdateValueByKey(String key, String value){
		try { 
			jedis.setex(key, 60, value);
		} catch(JedisConnectionException e){ 
			
		} finally { 
			//jedisPool.close();
		} 
	}
	
	public long createOrIncrLongValue(String key, long offset){
		
		long incrLongValue = 0L;

		try { 
			if(jedis.exists(key)){
				jedis.incrBy(key, offset);
			} else {
				jedis.setex(key, 60*60, String.valueOf(offset));
			}
			incrLongValue = Long.parseLong(jedis.get(key)); 
			
		} catch(JedisConnectionException e){ 
			System.out.println(e.getMessage());
		} finally { 
			//jedisPool.close();
		} 
		
		return incrLongValue;
	}
	
}
