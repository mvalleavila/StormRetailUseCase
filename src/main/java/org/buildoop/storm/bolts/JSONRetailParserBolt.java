package org.buildoop.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;






import org.buildoop.storm.tools.JSONRetailOperationParser;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import static backtype.storm.utils.Utils.tuple;

@SuppressWarnings("serial")
public class JSONRetailParserBolt implements IBasicBolt {

	@SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
    	
    	
    	/*Map<String,String> tupleValue = fillAuditAttributes(input.getString(0));
    	if (!tupleValue.isEmpty())
    		collector.emit(tuple(tupleValue));
    		*/
    	
    	
    }

    /*
    private Map<String,String> fillAuditAttributes(String input){
    	
    	/*
    	Map<String,String> tupleValue = new HashMap<String,String>();
    	String type = null;
    	
    	if (attributes.containsKey("type")){
    		type = (String)attributes.get("type");
    		
    		switch (type){
    			case "USER_LOGIN":
    			case "USER_LOGOUT":
    				if (attributes.get("res").equals("success"))
    				{
    					tupleValue.put("type", type);
    		   			tupleValue.put("host", (String)attributes.get("node"));
    					tupleValue.put("user", (String)attributes.get("username"));
    				}
    			default:
    		}    		
    	}    	
    	return tupleValue;
    	*//*
    	  private final static String input =
    			     "{" 
    			   + "  \"geodata\": [" 
    			   + "    {" 
    			   + "      \"id\": \"1\"," 
    			   + "      \"name\": \"Julie Sherman\","                  
    			   + "      \"gender\" : \"female\"," 
    			   + "      \"latitude\" : \"37.33774833333334\"," 
    			   + "      \"longitude\" : \"-121.88670166666667\""
    			   + "    }," 
    			   + "    {" 
    			   + "      \"id\": \"2\"," 
    			   + "      \"name\": \"Johnny Depp\","          
    			   + "      \"gender\" : \"male\"," 
    			   + "      \"latitude\" : \"37.336453\"," 
    			   + "      \"longitude\" : \"-121.884985\""
    			   + "    }" 
    			   + "  ]" 
    			   + "}";
    			   */
    	/*
    				Map<String,Object> shop_products =  new HashMap<>();


					try {
						JSONObject obj = new JSONObject(input);
						
						//JSONArray keys = obj.names();
						Iterator keys = obj.keys();
						
						String key = null;
						Object value = null;
						
						while ( keys.hasNext()){
							key = keys.next().toString();
							if (key.equals("products")){
								Map<String,Integer> productsMap = new HashMap<>();
								JSONArray productsJSON = obj.getJSONArray(key);
								for (int j=0; j< productsJSON.length(); j++){
									productsJSON.
								}
								shop_products.put("products", productsMap);
							}
								
							}
								
								
							value = obj.get
							shop_products.put(key, value)		
							
						}
					
						int size = keys.length();
						String op_type = obj.getString("op_type");
						
						switch (op_type){
						case "stock":
						case "tx":
						}

				
					for (int i = 0; i< size; i++){
										
						
						
					}
    			    final JSONArray geodata = obj.getJSONArray("geodata");
    			    final int n = geodata.length();
    			    for (int i = 0; i < n; ++i) {
    			      final JSONObject person = geodata.getJSONObject(i);
    			      System.out.println(person.getInt("id"));
    			      System.out.println(person.getString("name"));
    			      System.out.println(person.getString("gender"));
    			      System.out.println(person.getDouble("latitude"));
    			      System.out.println(person.getDouble("longitude"));
    			    }
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

    }*/
    

	public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tupleValue"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    

}