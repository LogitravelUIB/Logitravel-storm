package com.logitravel.showcase.hotels.bolts;

import java.net.UnknownHostException;
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
		
		// TODO Implement the following:
		
		// Given the city in the price object being processed,
		// extract from Mongo the best price saved for that city
		int city = price.getInt("City");
		BasicDBObject query = new BasicDBObject("_id", city);
		
		BasicDBObject result = (BasicDBObject) 
				this.bestPriceCollection.findOne(query);
		
		if(result == null){
			
			// If there is no price saved yet in Mongo for that city, save 
			// the current one
			BasicDBObject newDoc = new BasicDBObject("_id", city);
			newDoc.put("Popularity", price.getInt("Popularity"));
			newDoc.put("Name", price.getString("Name"));
			newDoc.put("SearchDate", price.getDate("SearchDate"));
			newDoc.put("Category", price.getInt("Category"));
			newDoc.put("Board", price.getString("Board"));
			newDoc.put("Price", Double.parseDouble(price.getString("Price")));
			this.bestPriceCollection.save(newDoc);
		}
		else{
			
			// If the price saved in Mongo is not as popular
			// as the price we are processing, replace it
			if(price.getInt("Popularity") > result.getInt("Popularity")){
				result.put("Popularity", price.getInt("Popularity"));
				result.put("Name", price.getString("Name"));
				result.put("SearchDate", price.getDate("SearchDate"));
				this.bestPriceCollection.save(result);
			}
			
			if(price.getDate("SearchDate").after(result.getDate("SearchDate"))){
				
			}
		}
		

		

		
		// If they have the same popularity, save in Mongo the most
		// recent price
		
		// If both prices have the same popularity and belong to the
		// same search, save the cheapest in Mongo
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
