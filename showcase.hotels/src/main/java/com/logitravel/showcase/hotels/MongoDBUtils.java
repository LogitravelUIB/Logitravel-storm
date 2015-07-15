package com.logitravel.showcase.hotels;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoDBUtils {
	
	public static DB getDB(String connectionString) throws UnknownHostException{
		MongoClientURI uri = new MongoClientURI(connectionString);
		MongoClient client = new MongoClient(uri);;
		
		return client != null ? client.getDB(uri.getDatabase()) : null;
	}	
}
