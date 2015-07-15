package com.logitravel.showcase.hotels.spouts;

import java.net.UnknownHostException;
import java.util.Map;

import com.logitravel.showcase.hotels.ConfigurationManager;
import com.logitravel.showcase.hotels.MongoDBUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class PricesQueue extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	private DBCollection queueCollection;
	
	public void nextTuple() {
		// Obtain the first non processed document
		DBObject pClause = new BasicDBObject("p", false);
		DBObject pExistClause = new BasicDBObject("p", new BasicDBObject("$exists", false));
		
		BasicDBList or = new BasicDBList();
		or.add(pExistClause);
		or.add(pClause);
		
		DBObject query = new BasicDBObject("$or", or);
		
		DBObject update = new BasicDBObject("$set", new BasicDBObject("p", true));
		DBObject retrieved = null;
		
		// If the collection has no elements to process,
		// wait 3 seconds before making another query.
		if(this.queueCollection == null 
				|| (retrieved = this.queueCollection.findAndModify(query, update)) == null)
		{
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) { 
				e.printStackTrace();
			}			
		}
		else{
			// Emit the tuple to be processed by the bolts
			// In this case, this tuple is a complete DBObject from
			// MongoDB.
			BasicDBObject doc = (BasicDBObject) retrieved;			
			if(doc != null)				
				this.collector.emit(new Values(doc));							
		}
	}

	@SuppressWarnings("rawtypes")
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		this.collector = collector;

		DB database = null;
		try {
			database = MongoDBUtils.getDB(ConfigurationManager.getInstance()
					.getValue("mongocol.pricesqueue.uri"));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		if(database != null)
			this.queueCollection = database.getCollection(ConfigurationManager.getInstance()
					.getValue("mongocol.pricesqueue.collection"));				
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("document"));
	}
}
