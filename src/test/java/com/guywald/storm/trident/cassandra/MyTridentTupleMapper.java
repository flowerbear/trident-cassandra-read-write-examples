package com.guywald.storm.trident.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

import com.hmsonline.storm.cassandra.bolt.mapper.TridentTupleMapper;
import com.hmsonline.storm.cassandra.exceptions.TupleMappingException;

public class MyTridentTupleMapper implements TridentTupleMapper<Integer, String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    private Fields fields;
    private String keyspace;

    public MyTridentTupleMapper(String keyspace, Fields fields){
        this.fields = fields;
        this.keyspace = keyspace;
    }	
	
	public Class<String> getColumnNameClass() {
		return String.class;
	}

	public Class<String> getColumnValueClass() {
		return String.class;
	}

	public Class<Integer> getKeyClass() {
		return Integer.class;
	}

	public String mapToColumnFamily(TridentTuple arg0) throws TupleMappingException {
		return "test_table";
	}
	
	public Map<String, String> mapToColumns(TridentTuple tuple) throws TupleMappingException {
		HashMap<String, String> retval = new HashMap<String, String>();
        for(String field : this.fields.toList()){
        	String value = tuple.getStringByField(field);
            retval.put(field, value);
        }		
		return retval;
	}

	public List<String> mapToColumnsForLookup(TridentTuple arg0) throws TupleMappingException {
		// TODO Auto-generated method stub
		return null;
	}

	public String mapToEndKey(TridentTuple arg0) throws TupleMappingException {
		// TODO Auto-generated method stub
		return null;
	}

	public String mapToKeyspace(TridentTuple arg0) {
		return this.keyspace;
	}

	public Integer mapToRowKey(TridentTuple arg0) throws TupleMappingException {
		// TODO Auto-generated method stub
		return null;
	}

	public String mapToStartKey(TridentTuple arg0) throws TupleMappingException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean shouldDelete(TridentTuple arg0) {
		// TODO Auto-generated method stub
		return false;
	}

}
