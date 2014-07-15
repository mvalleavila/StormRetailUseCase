package org.buildoop.storm.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class RetailOperationTools {
    
    public static IncrementAndOrderParams getProductIncrementOrderParams(String stockRowKey, int txTemperature, Map<String,String> product,
    		HTable stockTable,HTable stockTempTable)
    {
    	int productTxQuantity = Integer.parseInt(product.get("quantity"));
		Result resultRow;
		int productStock, productMinStock, orderFlag = 0;
		IncrementAndOrderParams incrementAndOrder = new IncrementAndOrderParams();
		
		try {
			resultRow = HBaseTools.getRow(stockTable,stockRowKey);
			productStock = HBaseTools.getCellIntValue(resultRow,"Stock","stock","Long");
			orderFlag = HBaseTools.getCellIntValue(resultRow,"Stock","stock","Int");
		} catch (IOException e) {
			//TODO: Comprobar que ocurre cuando no encuentra la fila ¡¡ DEBE CONTINUAR con la siguiente tupla !!
			System.out.println("WARN: ProccesRetailTransactionBolt - " + " get Row " + stockRowKey + 
					" from table " + stockTable + " not found");
			e.printStackTrace();
			return null;
		} catch (HBaseException e){
			System.out.println("WARN: ProccesRetailTransactionBolt - " + " get Value Stock:stock from row" + stockRowKey + 
					",table " + stockTable + " is corrupt");
			e.printStackTrace();
			return null;
		}
		
		String stockTempRowKey = stockRowKey + "|" + txTemperature;
		
		if (productStock <= 0)
		{
			System.out.println("WARN: ProccesRetailTransactionBolt - " + stockRowKey + " stock is less than one");
			return null;
		}

		try {
			resultRow = HBaseTools.getRow(stockTempTable,stockTempRowKey);
			productMinStock = HBaseTools.getCellIntValue(resultRow,"MinStock","stock","String");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("WARN: ProccesRetailTransactionBolt - " + " get Row " + stockTempRowKey + 
					" from table " + stockTable + " not found");
			productMinStock = -1;
		} catch (HBaseException e){
			System.out.println("WARN: ProccesRetailTransactionBolt - " + " get Value MinStock:stock from row " + stockRowKey + 
					", table " + stockTable + " is corrupt");
			productMinStock = -1;

			
		}
		
		int orderProductQuantity = orderProductQuantity(productStock, productMinStock, productTxQuantity);
		incrementAndOrder.setOrderQuantity(orderProductQuantity);
		incrementAndOrder.setOrderFlag(orderFlag);
		
		if ((productStock - productTxQuantity) < 0)
		{
			System.out.println("WARN: ProccesRetailTransactionBolt - " + stockRowKey + 
					" more units sold in transaction that in stock, setting product stock to 0");
			incrementAndOrder.setIncrement(productStock*-1);
		}
		else
		{
			System.out.println("INFO: ProccesRetailTransactionBolt -  " + stockRowKey + 
					"setting to " + String.valueOf(productStock - productTxQuantity));
			incrementAndOrder.setIncrement(productTxQuantity*-1);
		}
		return incrementAndOrder;
    }
    
    
    private static int orderProductQuantity(int productStock, int productMinStock, int productTxQuantity)
    {
    	int orderQuantity=0;
    	
    	if ((productStock - productTxQuantity) < 0)
    		orderQuantity = productMinStock + 10;
    	else if ((productStock - productTxQuantity) < productMinStock)
    		orderQuantity = productMinStock - (productStock- productTxQuantity) + 10;
    	
    	return orderQuantity;
    }
    
    public static String buildOrderProductInfo(String shopName, String shopId, String productName, int orderQuantity)
    {
    	Map<String,Object> orderOperationInfo = new HashMap<String, Object>(5);
    	orderOperationInfo.put("op_type", "order");
    	orderOperationInfo.put("shop_name", shopName);
    	orderOperationInfo.put("shop_id", shopId);
    	orderOperationInfo.put("timestamp", new Date().getTime());
    	Map<String,Object> productInfo = new HashMap<String,Object>(1);
    	productInfo.put("product", productName);
    	productInfo.put("quantity", orderQuantity);
    	ArrayList<Map<String,Object>> productsArray = new ArrayList<Map<String,Object>>(1);
    	productsArray.add(productInfo);
    	orderOperationInfo.put("products", productsArray);
    			
    	
    	return JSONRetailOperation.encodeJsonOrderOperation(orderOperationInfo);
    }
}
