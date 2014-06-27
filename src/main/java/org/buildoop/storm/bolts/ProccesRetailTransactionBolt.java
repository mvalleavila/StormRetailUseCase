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
        declarer.declare(new Fields("shop|product", "stock", "order"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    //TODO: Sacar esta funcion a transaction.tools ¿Clase -> Objeto transactionInfo?
    private void proccesTransactionOperation(Map<String,Object> operationInfo, BasicOutputCollector collector) throws IOException
    {
		ArrayList<Map<String,String>> allProducts = (ArrayList<Map<String,String>>)operationInfo.get("products");
    	String shopName = operationInfo.get("shop_name").toString();
    	Map<String,String> product = null;
    	String stockRowKey, stockTempRowKey, productName = null;
    	long productStock,productMinStock = 0; 
    	int productTxQuantity, order = 0;
    	int txTemperature = Integer.parseInt((String) operationInfo.get("temperature"));
    	
    	HTable stockTable = openTable(this.hbaseStockTableName);
    	HTable stockTempTable = openTable(this.hbaseStockTempTableName);
    	Result resultRow = null;
		
		for (int i=0; i< allProducts.size(); i++)
		{
			//Procces product in position i
			product = allProducts.get(i);
			if (product.containsKey("product") && product.containsKey("quantity"))
			{
				System.out.println("--------------------------- ProccesRetailTransactionBolt.proccesAllProducts--> Producto " + i);
				productName = product.get("product");
				stockRowKey = shopName + "|" + productName;
				stockTempRowKey = shopName + "|" + productName + "|" + String.valueOf(txTemperature);
								
				try {
					productTxQuantity = Integer.parseInt(product.get("quantity"));
					resultRow = getRow(stockTable,stockRowKey);
					productStock = getCellLongValue(resultRow,"Stock","stock","Long");
					order = (int) getCellLongValue(resultRow,"Stock","order","Int");
					
					if (productStock <= 0)
					{
						// Don't do anything
						System.out.println("WARN: ProccesRetailTransactionBolt - Shop|Product:  " + stockRowKey + " less than one");
					}
					else if ((productStock - productTxQuantity) < 0)
					{
						// Set product stock to 0
						System.out.println("WARN: ProccesRetailTransactionBolt - Shop|Product " + stockRowKey + 
								"minus sell in transaction less than 0, setting product stock to 0");
						// Read product minimum stock based in stockTemp hbase table
						resultRow = getRow(stockTempTable,stockTempRowKey);
						productMinStock = getCellLongValue(resultRow,"MinStock","stock","String");
						
						/* TODO: FUERA ESTA FUNCION!! 
						 * Tratar el shop_id que viene en la transaccion, para construir el json de peticion de stock 
						¿timestamp lo genera en el momento de pedir el stock? */
						long pedido = productMinStock + 10;
						System.out.println("PEDIDO ----- tienda: "+ shopName + " product: " + productName + " quantity: " + pedido);
						

						collector.emit(tuple(stockRowKey, (productStock*-1), order));
					}
					else
					{
						// Decrement product stock
						System.out.println("INFO: ProccesRetailTransactionBolt - Shop|Product " + stockRowKey + 
								"setting to " + String.valueOf(productStock - productTxQuantity));
						// Read product minimum stock based in stockTemp hbase table
						resultRow = getRow(stockTempTable,stockTempRowKey);
						productMinStock = getCellLongValue(resultRow,"MinStock","stock","String");
						if (productStock - productTxQuantity < productMinStock)
						{
							// Send product 
							/* TODO: FUERA ESTA FUNCION!! 
							 * Tratar el shop_id que viene en la transaccion, para construir el json de peticion de stock 
							¿timestamp lo genera en el momento de pedir el stock? */
							long pedido = productMinStock - (productStock- productTxQuantity) + 10;
							System.out.println("PEDIDO ----- tienda: "+ shopName + " product: " + productName + " quantity: " + pedido);
						}
						
						collector.emit(tuple(stockRowKey, (productTxQuantity)*-1,order));
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
		
		stockTable.close();
		stockTempTable.close();
    }
    
    //TODO: Sacar este metodo a un módulo aparte y hacerlo generico --> Extension de alguna clase de HBase? Tools de HBase??
    private long getCellLongValue(Result result, String columFamily,String qualifier, String qualifierType) throws IOException
    {
		
		byte [] productStock = result.getValue(Bytes.toBytes(columFamily),Bytes.toBytes(qualifier));
		long productStockQuantity = 0;
		
		switch (qualifierType)
		{
		case "String":
			productStockQuantity = Long.parseLong((Bytes.toString(productStock)));
			break;
		case "Int":
			productStockQuantity = Bytes.toInt(productStock);
			break;
		case "Long":
			productStockQuantity = Bytes.toLong(productStock);
			break;
		default:
			System.out.println("WARN: getProductStock - Bad qualifierType: " + qualifierType);
			return -1;				
		}
		
    	if (productStock!=null && productStockQuantity >= 0)	
    		return productStockQuantity;
    	else
    	{
    		//TODO: Sacar a log4j, Especificar columna
    		System.out.println("WARN: getProductStock - Bad column: " + qualifierType);
    		return -1;
    	}
    }
    
    private Result getRow(HTable hbaseTable, String hbaseRowKey) throws IOException
    {
    	Get get = new Get(Bytes.toBytes(hbaseRowKey));
		return hbaseTable.get(get);
    }
    
    private HTable openTable(String hbaseTableName) throws IOException
    {
    	Configuration config = HBaseConfiguration.create();
		return new HTable(config, hbaseTableName);
    }
}
