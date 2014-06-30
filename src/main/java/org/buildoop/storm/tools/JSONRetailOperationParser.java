package org.buildoop.storm.tools;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class JSONRetailOperationParser {
	
    @SuppressWarnings("unchecked")
	public static Map<String,Object> parseRetailtOperationInput(String input) {

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
			String key = null;
			String value = null;
			
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
				if (key.equals("products"))
				{

					productsJSONArray = operationJSONobject.getJSONArray(key);
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
}
