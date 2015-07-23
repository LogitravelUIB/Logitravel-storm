package com.logitravel.showcase.hotels.bolts;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

import com.logitravel.showcase.hotels.ConfigurationManager;
import com.logitravel.showcase.hotels.MongoDBUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class BestCityPrice extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;	
	private DBCollection bestPriceCollection;
	
	public void execute(Tuple input) {
		BasicDBObject price = (BasicDBObject) input.getValueByField("price");
				
		// Given the city in the price object being processed,
		// extract from Mongo the best price saved for that city
		int city = price.getInt("City");
		BasicDBObject result = getBestPriceForCity(city);
		
		if(result == null){
			
			// If there is no price saved yet in Mongo for that city, save 
			// the current one
			BasicDBObject newDoc = new BasicDBObject("_id", city);
			updateCurrentInformation(price, newDoc);
		}
		else{
			int currentPricePopularity = price.getInt("Popularity");
			int savedPricePopularity = result.getInt("Popularity");
			
			Date currentPriceDate = price.getDate("SearchDate");
			Date savedPriceDate = result.getDate("SearchDate");
			
			double currentPriceAmount = price.getDouble("Price");
			double savedPriceAmount = result.getDouble("Price");
			
			// If the price saved in Mongo is not as popular
			// as the price we are processing, replace it
			if(currentPricePopularity > savedPricePopularity){
				updateCurrentInformation(price, result);				
			}						
			else if(currentPricePopularity == savedPricePopularity){
				if(currentPriceDate.after(savedPriceDate)){
					
					// If they have the same popularity, save in Mongo the most
					// recent price
					updateCurrentInformation(price, result);
				}
				else if(currentPriceDate.equals(savedPriceDate)
						&& currentPriceAmount < savedPriceAmount){
					
					// If both prices have the same popularity and belong to the
					// same search, save the cheapest in Mongo
					updateCurrentInformation(price, result);
				}
			}
		}
	}

	private BasicDBObject getBestPriceForCity(int city) {
		BasicDBObject query = new BasicDBObject("_id", city);
		
		BasicDBObject result = (BasicDBObject) 
				this.bestPriceCollection.findOne(query);
		return result;
	}

	private void updateCurrentInformation(BasicDBObject price, BasicDBObject result) {
		result.put("Popularity", price.getInt("Popularity"));
		result.put("Name", price.getString("Name"));
		result.put("SearchDate", price.getDate("SearchDate"));
		result.put("Board", price.getString("Board"));
		result.put("Category", price.getInt("Category"));
		result.put("Price", price.getDouble("Price"));
		this.bestPriceCollection.save(result);
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext arg1, 
			OutputCollector arg2) {
		
		DB database = null;
		try {
			database = MongoDBUtils.getDB(ConfigurationManager.getInstance()
					.getValue("mongocol.bestprices.uri"));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		if(database != null)
			this.bestPriceCollection = database.getCollection(ConfigurationManager.getInstance()
					.getValue("mongocol.bestprices.collection"));	
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}
}
