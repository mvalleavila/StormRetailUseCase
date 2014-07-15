/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.buildoop.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;

@SuppressWarnings("serial")
public class ProccesRetailStockBolt implements IBasicBolt {

    @SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    /*
     * Just output the word value with a count of 1.
     * The HBaseBolt will handle incrementing the counter.
     */
	@Override
    public void execute(Tuple input, BasicOutputCollector collector){
		
		System.out.println("---------------------- ENTRO EN execute ProccesRetailStockBolt");
		
		@SuppressWarnings("unchecked")
		Map<String,Object> operationInfo = (Map<String, Object>) input.getValues().get(0);
    	
    	String op_type = operationInfo.get("op_type").toString();
    	
    	if (op_type.equals("stock"))
    	{
			proccesStockOperation(operationInfo, collector);
    	}
    	else{
    		System.out.println("ERROR: ProccesRetailTransactionBolt -  op_type not tx");
    	}
    }
	@Override
    public void cleanup() {

    }
	
	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("shop|product", "stock", "order"));
    }
	
    
    private void proccesStockOperation(Map<String,Object> operationInfo, BasicOutputCollector collector)
    {
    	
    	@SuppressWarnings("unchecked")
		ArrayList<Map<String,String>> allProducts = (ArrayList<Map<String,String>>)operationInfo.get("products");
    	int numProducts=allProducts.size();
    	String shopName = operationInfo.get("shop_name").toString();
    	String rowKey, productName=null;
    	Map<String,String> product = null;
    	int productStockOpQuantity = 0;
    	
    	for (int i=0; i < numProducts; i++)
    	{
			product = allProducts.get(i);	
			productName = product.get("product");
			
			rowKey = shopName + "|" + productName;
			
			System.out.println("-----------11111----------- BUCLE posicion " + i + " ENTRO EN proccesStockOperation ProccesRetailStockBolt");
			
			if (productName == null){
				System.out.println("ERROR: proccesStockOperation -  product in position " + i + " name not found! "
		   				+ "Check JSON!!");
			}
			else {
				System.out.println("-----------22222----------- BUCLE posicion " + i + " ENTRO EN proccesStockOperation ProccesRetailStockBolt");
				try{
					productStockOpQuantity = Integer.parseInt(product.get("quantity"));
				} catch (NumberFormatException e){
			   		System.out.println("ERROR: proccesStockOperation -  product " + productName + " quantity not found! "
			   				+ "Check JSON!!");
				}
	
				if (productStockOpQuantity > 0)
					collector.emit(tuple(rowKey,productStockOpQuantity, 0));
			}
    	}
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
