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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

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
    	
    	/* 
    		TODO: Hacer recorrido de los productos -> ¿SACAR A UNA FUNCION EN TOOLS?
    	*/
    	Map<String,Object> operationInfo = (Map<String, Object>) input.getValues().get(0);
    	
    	String op_type = operationInfo.get("op_type").toString();
    	
    	if (op_type.equals("tx"))
    	{
    		// Procces all products in transaction
    		proccesAllProducts(operationInfo, collector);
    	}
    	else{
    		System.out.println("ERROR: ProccesRetailTransactionBolt -  op_type not tx");
    	}
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("shop-product", "stock"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    private void proccesAllProducts(Map<String,Object> operationInfo, BasicOutputCollector collector)
    {
		ArrayList<Map<String,String>> allProducts = (ArrayList<Map<String,String>>)operationInfo.get("products");
    	String shopName = operationInfo.get("shop_name").toString();
    	Map<String,String> product = null;
    	String stockRowKey, stockTempRowKey, productName = null;
    	int productStock, productTxQuantity, productMinStock = 0;
    	int txTemperature = (int) operationInfo.get("temperature");
		
		for (int i=0; i< allProducts.size(); i++)
		{
			//Procces product in position i
			product = allProducts.get(i);
			if (product.containsKey("product") && product.containsKey("quantity"))
			{
				productName = product.get("product");
				stockRowKey = shopName + "|" + productName;
				stockTempRowKey = shopName + "|" + productName + "|" + String.valueOf(txTemperature);				
				
				try {
					productTxQuantity = Integer.parseInt(product.get("quantity"));
					productStock = getProductStock(this.hbaseStockTableName,stockRowKey,"Stock","stock");
					if (productStock <= 0)
					{
						// Don't do anything
						System.out.println("WARN: ProccesRetailTransactionBolt - Shop|Product " + stockRowKey + "less than one");
					}
					else if ((productStock - productTxQuantity) < 0)
					{
						// Set product stock to 0
						System.out.println("WARN: ProccesRetailTransactionBolt - Shop|Product " + stockRowKey + 
								"minus sell in transaction less than 0, setting product stock to 0");
						// Read product minimum stock based in stockTemp hbase table
						productMinStock = getProductStock(this.hbaseStockTempTableName,stockTempRowKey,"MinStock","stock");
						// sendEmailToDistributor
						collector.emit(tuple(stockRowKey, (productStock*-1)));
					}
					else
					{
						// Decrement product stock
						System.out.println("INFO: ProccesRetailTransactionBolt - Shop|Product " + stockRowKey + 
								"setting to " + String.valueOf(productStock - productTxQuantity));
						// Read product minimum stock based in stockTemp hbase table
						
						collector.emit(tuple(stockRowKey, (productStock - productTxQuantity)*-1));
					}
				} catch (IOException e) {
					System.out.println("WARN: ProccesRetailTransactionBolt - Product " + i + "IOException");
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("WARN: ProccesRetailTransactionBolt - Product " + i + "haven't got product/quantity key");
			}
		}
    }
    
    private int getProductStock(String hbaseTableName, String hbaseRowKey, String columFamily,
    		String qualifier) throws IOException
    {
    	Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, hbaseTableName);
		Get get = new Get(Bytes.toBytes(hbaseRowKey));
		Result result = table.get(get);
		byte [] productStock = result.getValue(Bytes.toBytes(columFamily),Bytes.toBytes(qualifier));
		table.close();
    	if (productStock!=null && Bytes.toLong(productStock) >= 0)    	
    		return Bytes.toInt(productStock);
    	else
    		return -1;
    }
}
