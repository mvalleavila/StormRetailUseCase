package org.buildoop.storm.tools;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class JSONRetailOperation {
	
    @SuppressWarnings("unchecked")
	public static Map<String,Object> parseJsonInputRetailtOperation(String input) {

    	/*
    	 * Data Structure:
    	 * operationInfo -> Main structure, containing a Map with properties of the JSON transaction:
    	 * 		e.g. key = "op_type", value = "tx"
    	 * 
    	 * JSON archives relative to sell transactions or stock supply have an array with the products,
    	 * the structure of this products array is an element of operationInfo map with key "products"
    	 * and the value is an ArrayList named productsList
    	 * productList -> Array List in each element is a Map named productAttributes, each element is the
    	 * 				  product information attributes
    	 * productAttributes -> Map with specific product information
    	 * 		e.g key = "product", value = "Milk ; key = "quantity", value = "25" 
    	 */
    	
    	
    	Map<String,Object> operationInfo =  new HashMap<>();
    	
		try {
			
			// Take entire line an insert in JSONObject
			JSONObject operationJSONobject = new JSONObject(input);
			// Iterator to browse main JSONobject
			Iterator<String> keys = operationJSONobject.keys();
			
			// General variables for not products information
			String key, value = null;
			Object valueObject = null;
			
			// Products information variables, from container structure to specific info:
			// - productList (value of main structure with key "products"), array list in which each 
			//                 element is a Map containing one product attributes
			ArrayList<Map<String,String>> productsList = new ArrayList<>();
			
			// Auxiliary structure to save entire JSON products array
			JSONArray productsJSONArray = new JSONArray();
			
			// - productJsonObject: Auxiliary structure to save product JSON specific attributes
			JSONObject productJsonObject = null;
			// - productAttributes: map containing one product information  
			Map<String,String> productAttributes = new HashMap<>();
			
			//Iterator to browse specific attributes of one product
			Iterator<String> productKeys = null;
			// Auxiliary variable for save specific product attribute key
			String productKey = null;

			while ( keys.hasNext())
			{				
				key = keys.next().toString();
				valueObject = operationJSONobject.get(key);
				if (valueObject instanceof JSONArray)
				{
					productsJSONArray = (JSONArray) valueObject;
					for (int i=0; i<productsJSONArray.length();i++)
					{
						productJsonObject = productsJSONArray.getJSONObject(i);
						productKeys = productJsonObject.keys();
						
						while ( productKeys.hasNext())
						{
							productKey = productKeys.next().toString();
							
							value = productJsonObject.getString(productKey);
							productAttributes.put(productKey,value);
						}
						productsList.add(productAttributes);
					}
					operationInfo.put(key,productsList);
				}
				else
				{
					value = operationJSONobject.getString(key);
					operationInfo.put(key,value);
				}
			}
			} catch (JSONException e) {
				System.out.println("Json Parse error: Check correct structure of Json operation information");
				e.printStackTrace();
			}
		return operationInfo;
    }
    
    @SuppressWarnings("unchecked")
	public static String encodeJsonOrderOperation(Map<String,Object> operationInfo)
    {
    	String key=null;
    	Object value=null;
    	ArrayList<Map<String,Object>> array;
    	Map<String,Object> arrayElement;
    	JSONObject json = new JSONObject();
    	JSONArray jsonArray = new JSONArray();
    	
    	try
    	{
	    	for (Entry<String,Object> entry : operationInfo.entrySet())
	    	{
	    		key = entry.getKey();
	    		value = entry.getValue();
	    		if (value instanceof ArrayList)
	    		{
	    			array = (ArrayList<Map<String,Object>>)value;
	    			for (int i=0; i< array.size(); i++)
	    			{
	    				arrayElement = array.get(i);
	    				jsonArray.put(i, arrayElement);
	    			}
	    			json.put(key,jsonArray);
	    		}
	    		else
	    		{
	    			json.put(key,value);
	    		}
	    	}
    	}catch(JSONException e){
    		System.out.println("Json Encoding error: PRINT OPERATION INFO STRUCTURE!!");
			e.printStackTrace();
    	}
    	
    	return json.toString();
    }
}
