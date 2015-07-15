package com.logitravel.showcase.hotels;

import com.logitravel.showcase.hotels.bolts.BestCityPrice;
import com.logitravel.showcase.hotels.spouts.PricesQueue;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class Topology 
{
    public static void main( String[] args )
    {
        TopologyBuilder builder = new TopologyBuilder();
        
        // Set the spout in the topology
        builder.setSpout("queue", new PricesQueue());
        
        // Build your topology here
		builder.setBolt("bestCityPrice", new BestCityPrice())
			.shuffleGrouping("queue");
        
		Config conf = new Config();
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
            
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(120000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }        
    }
}
