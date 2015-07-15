package com.logitravel.showcase.hotels;

import java.util.Properties;

public class ConfigurationManager {
	private static ConfigurationManager instance = null;
	private static Object lock = new Object();
	private static String propertyFile = "topology.properties";
	
	protected Properties prop = null;
	
	private ConfigurationManager(){
		try {		    
			//Properties
		    this.prop = new Properties();
		    Properties propBase = new Properties();
		    propBase.load(ConfigurationManager.class.getClassLoader().getResourceAsStream(ConfigurationManager.propertyFile));
		    this.prop.putAll(propBase);
		    
		} 
		catch (Exception ex) {
		    throw new RuntimeException(ex);
		}		
	}
	
	public static ConfigurationManager getInstance(){
		if(instance == null){
			synchronized(lock){
				if(instance == null)
					instance = new ConfigurationManager();
			}
		}			
		return instance;
	}
	
	public String getValue(String key){
		return prop.getProperty(key);		
	}
}
