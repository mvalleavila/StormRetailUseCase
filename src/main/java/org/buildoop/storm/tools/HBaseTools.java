package org.buildoop.storm.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTools {
	
    
    public static HTable openTable(String hbaseTableName) throws IOException
    {
    	Configuration config = HBaseConfiguration.create();
		return new HTable(config, hbaseTableName);
    }
    
    public static Result getRow(HTable hbaseTable, String hbaseRowKey) throws IOException
    {
    	Get get = new Get(Bytes.toBytes(hbaseRowKey));
		return hbaseTable.get(get);
    }
    
	 //TODO: Sacar este metodo a un módulo aparte y hacerlo generico --> Extension de alguna clase de HBase? Tools de HBase??
    public static int getCellIntValue(Result result, String columFamily,String qualifier, String hbaseQualifierType) throws IOException, HBaseException
    {
		
		byte [] cellValue = result.getValue(Bytes.toBytes(columFamily),Bytes.toBytes(qualifier));
		int cellIntValue = 0;
		
		switch (hbaseQualifierType)
		{
		case "String":
			cellIntValue = (int) Long.parseLong((Bytes.toString(cellValue)));
			break;
		case "Int":
			cellIntValue = Bytes.toInt(cellValue);
			break;
		case "Long":
			cellIntValue = (int) Bytes.toLong(cellValue);
			break;
		default:
			System.out.println("WARN: getProductStock - Bad qualifierType: " + hbaseQualifierType);
			return -1;				
		}
		
    	if (cellValue!=null && cellIntValue >= 0)	
    		return cellIntValue;
    	else
    	{
    		throw new HBaseException("WARN: getProductStock - Bad column: " + hbaseQualifierType);
    	}
    }

}
