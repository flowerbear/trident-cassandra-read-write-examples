package com.guywald.storm.trident.cassandra;

import java.util.HashMap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.trident.CassandraMapState;
import com.hmsonline.storm.cassandra.trident.CassandraMapState.Options;
import com.hmsonline.storm.cassandra.trident.CassandraUpdater;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CassandraTest {

	private static Logger LOG = LoggerFactory.getLogger(CassandraTest.class);

	public static class AddId extends BaseFunction {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public static int id = 1002;
		
	    public void execute(TridentTuple tuple, TridentCollector collector) {
	    	collector.emit(new Values(id++));
	    }
	    
	}	
	
	@Test
	public void test() throws InterruptedException {
		LOG.info("Start cassandra test");
		
		String keyspace = "my_keyspace";
	
		
        @SuppressWarnings("unchecked")
		FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("sentence"), 3, 
                new Values("the cow jumped over the moon"), 
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"), 
                new Values("how many apples can you eat"));
        spout.setCycle(false);
        
        TridentTopology topology = new TridentTopology();
        
        HashMap<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        clientConfig.put(StormCassandraConstants.CASSANDRA_STATE_KEYSPACE, keyspace);
        Config config = new Config();
        config.setMaxSpoutPending(25);
        config.put("cassandra.config", clientConfig);        
        
        Options options = new Options<Object>();
        StateFactory cassandraStateFactory = CassandraMapState.nonTransactional(options);
        
        Fields fields = new Fields("id", "sentence"); // same order as in table in cassandra db
        MyTridentTupleMapper tupleMapper = new MyTridentTupleMapper(keyspace, fields);
        CassandraUpdater updater = new CassandraUpdater(tupleMapper);
        
        
		TridentState wordCounts = topology.newStream("spout1", spout)
        		.each(new Fields("sentence"), new AddId(), new Fields("id"))
        		.partitionPersist(cassandraStateFactory, fields, updater);
		LOG.info("Here");
		LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology.build());

        while(true) {
        	
        }
        
	}

}
