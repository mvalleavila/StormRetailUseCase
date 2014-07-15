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

import org.apache.hadoop.hbase.client.HTable;
import org.buildoop.storm.tools.IncrementAndOrderParams;
import org.buildoop.storm.tools.HBaseTools;
import org.buildoop.storm.tools.RetailOperationTools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;

@SuppressWarnings("serial")
public class ProccesRetailTransactionBolt implements IBasicBolt {
	
	private String hbaseStockTableName;
	private String hbaseStockTempTableName;
	
	public ProccesRetailTransactionBolt(String hbaseStockTableName, String hbaseStockTempTableName){
		this.hbaseStockTableName = hbaseStockTableName;
		this.hbaseStockTempTableName = hbaseStockTempTableName;
	}


    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
    }

    /*
     */
    public void execute(Tuple input, BasicOutputCollector collector){
    	
		System.out.println("---------------------- ENTRO EN execute ProccesRetailTransactionBolt");
    	
    	@SuppressWarnings("unchecked")
		Map<String,Object> operationInfo = (Map<String, Object>) input.getValues().get(0);
    	
    	String op_type = operationInfo.get("op_type").toString();
    	
    	if (op_type.equals("tx"))
    	{
    		try {
				proccesTransactionOperation(operationInfo, collector);
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	else{
    		System.out.println("ERROR: ProccesRetailTransactionBolt -  op_type not tx");
    	}
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("hbaseStream",new Fields("shop|product", "stock", "order"));
        declarer.declareStream("orderStream", new Fields("tupleValue"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    private void proccesTransactionOperation(Map<String,Object> operationInfo, BasicOutputCollector collector) throws IOException
    {
    	
    	@SuppressWarnings("unchecked")
		ArrayList<Map<String,String>> allProducts = (ArrayList<Map<String,String>>)operationInfo.get("products");
    	int numProducts=allProducts.size();
    	String shopName = operationInfo.get("shop_name").toString();
    	String shopId = operationInfo.get("shop_id").toString();
    	int txTemperature = Integer.parseInt((String) operationInfo.get("temperature")); 
    	IncrementAndOrderParams incrementAndOrderParams;
    	HTable stockTable = HBaseTools.openTable(this.hbaseStockTableName);
    	HTable stockTempTable = HBaseTools.openTable(this.hbaseStockTempTableName);
    	String rowKey, productName=null;
    	Map<String,String> product = null;
    	int orderQuantity = 0;
    	
    	for (int i=0; i < numProducts; i++)
    	{
    		product = allProducts.get(i);
        	productName = product.get("product");
        	rowKey = shopName + "|" + productName;
    		incrementAndOrderParams = RetailOperationTools.getProductIncrementOrderParams(rowKey,txTemperature,product,stockTable,stockTempTable);
    		    		
    		if (incrementAndOrderParams != null){
    			orderQuantity = incrementAndOrderParams.getOrderQuantity();
    			if ( orderQuantity > 0){
    				String orderProductInfo = RetailOperationTools.buildOrderProductInfo(shopName,shopId,productName,orderQuantity);
    				//TODO: Hacer pedido!! --> Stream para Order Bolt
    				collector.emit("orderStream",tuple(orderProductInfo));
    				//TODO: Estructura de productos para el pedido a ActiveMQ
    				collector.emit("hbaseStream",tuple(rowKey,incrementAndOrderParams.getIncrement(), 1));
    				
    			}
    			else{
    				collector.emit("hbaseStream",tuple(rowKey,incrementAndOrderParams.getIncrement(), incrementAndOrderParams.getOrderFlag()));
    			}
    		}
    	}
    	
		stockTable.close();
		stockTempTable.close();
    }
}
