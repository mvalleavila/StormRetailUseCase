package org.buildoop.storm.bolts;

import static backtype.storm.utils.Utils.tuple;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

import org.buildoop.storm.tools.JSONRetailOperationParser;;

@SuppressWarnings("serial")
public class JSONRetailParserBolt implements IBasicBolt {

	@SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
    	Map<String,Object> tupleValue = JSONRetailOperationParser.parseRetailtOperationInput(input.getString(0));

    	if (!tupleValue.isEmpty())
    	{
    		String opType = operationType(tupleValue);;
    		switch (opType)
    		{
    			case "stock":
    				collector.emit("stock",tuple(tupleValue));
    				break;
    			case "tx":
    				collector.emit("transaction",tuple(tupleValue));
    				break;
    			case "error":
    			default:
    				System.out.println("WARN org.buildoop.storm.bolts.JSONRetailParserBolt : Operation type " + opType + " not supported");
    				break;
    		}
    	}
    	
    }
    

	public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("transaction",new Fields("tupleValue"));
        declarer.declareStream("stock", new Fields("tupleValue"));
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