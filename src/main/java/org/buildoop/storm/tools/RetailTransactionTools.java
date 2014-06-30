package org.buildoop.storm.tools;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class RetailTransactionTools {
    
    public static IncrementAndOrderParams getProductIncrementOrder(String stockRowKey, int txTemperature, Map<String,String> product,
    		HTable stockTable,HTable stockTempTable)
    {
    	int productTxQuantity = Integer.parseInt(product.get("quantity"));
		Result resultRow;
		int productStock, productMinStock;
		IncrementAndOrderParams incrementAndOrder = new IncrementAndOrderParams();
		
		try {
			resultRow = HBaseTools.getRow(stockTable,stockRowKey);
			productStock = HBaseTools.getCellIntValue(resultRow,"Stock","stock","Long");
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
}
