package org.buildoop.storm.bolts;

import static backtype.storm.utils.Utils.tuple;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

import org.buildoop.storm.tools.JSONRetailOperation;;

@SuppressWarnings("serial")
public class JSONRetailParserBolt implements IBasicBolt {

	@SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
    	
		System.out.println("---------------------- ENTRO EN execute JSONRetailParserBolt");
    	
    	Map<String,Object> tupleValue = JSONRetailOperation.parseJsonInputRetailtOperation(input.getString(0));
    	String sourceComponent = input.getSourceComponent();

    	if (!tupleValue.isEmpty())
    	{
    		System.out.println("---------------------- ENTRO EN switch JSONRetailParserBolt");
    		String opType = operationType(tupleValue);;
    		switch (opType)
    		{
    			case "stock":
    				if (sourceComponent.equals("KafkaStockSpout"))
    					collector.emit("stockStream",tuple(tupleValue));
    				else
    					System.out.println("ERROR org.buildoop.storm.bolts.JSONRetailParserBolt : Topic stock not sending stock operations");    				
    				break;
    			case "tx":
    				if (sourceComponent.equals("KafkaTransactionSpout"))
    					collector.emit("transactionStream",tuple(tupleValue));
    				else
    					System.out.println("ERROR org.buildoop.storm.bolts.JSONRetailParserBolt : Topic transaction not sending transaction operations");
    				break;
    			case "error":
    				System.out.println("WARN org.buildoop.storm.bolts.JSONRetailParserBolt : Operation type not found in input JSON");
    				break;
    			default:
    				System.out.println("WARN org.buildoop.storm.bolts.JSONRetailParserBolt : Operation type " + opType + " not supported");
    				break;
    		}
    	}
    	
    }
    

	public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("transactionStream",new Fields("tupleValue"));
        declarer.declareStream("stockStream", new Fields("tupleValue"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    private String operationType(Map<String,Object> input)
    {
    	if (input.containsKey("op_type"))
    	{
    		return input.get("op_type").toString();
    	}
    	else
    	{
    		return "error";
    	}
    }
}