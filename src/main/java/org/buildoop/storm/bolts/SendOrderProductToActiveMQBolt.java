package org.buildoop.storm.bolts;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import org.buildoop.storm.tools.ActiveMQBoltConfig;
import org.fusesource.stomp.jms.*;

import javax.jms.*;

@SuppressWarnings("serial")
public class SendOrderProductToActiveMQBolt implements IBasicBolt {
	
	//TODO: Implementar
	private ActiveMQBoltConfig activeMQConfig;
	private Connection connection;
	private Session session;
	private MessageProducer producer;
	
	public SendOrderProductToActiveMQBolt(ActiveMQBoltConfig activeMQConfig) {
		this.activeMQConfig=activeMQConfig;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
		System.out.println("---------------------- ENTRO EN prepare SendOrderProductBolt");
		
		String user = activeMQConfig.getActiveMQUser();
        String password = activeMQConfig.getActiveMQPassword();
        String host = activeMQConfig.getActiveMQHost();
        int port = activeMQConfig.getActiveMQPort();
        
        String destination = activeMQConfig.getActiveMQDestination();


        StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
        factory.setBrokerURI("tcp://" + host + ":" + port);
        
		try {


        connection = factory.createConnection(user, password);
	
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(destination);
		producer = session.createProducer(dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		
        
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

        String body = input.getString(0);
        

		try {

            TextMessage msg = session.createTextMessage(body);
            producer.send(msg);

		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void cleanup() {

		try {
        connection.close();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
