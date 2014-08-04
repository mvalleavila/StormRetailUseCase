package org.buildoop.storm.tools;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.buildoop.storm.bolts.JSONRetailParserBolt;
import org.buildoop.storm.bolts.ProccesRetailStockBolt;
import org.buildoop.storm.bolts.ProccesRetailTransactionBolt;
import org.buildoop.storm.bolts.SendOrderProductToActiveMQBolt;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class RetailTopologyTools{
	
	private static StormTopology buildTopology(Properties properties) {		
		//TODO: IMPORTANTE! TRATAR MENSAJES DE ACK Y FAIL ENTRE LOS BOLT
		
		// Load properties for the storm topology
		String kafkaTransactionTopic = properties.getProperty("kafka.transaction.topic");
		String kafkaStockTopic = properties.getProperty("kafka.stock.topic");
		String hbaseStockTable = properties.getProperty("hbase.stock.table.name");
		String hbaseStockTempTable = properties.getProperty("hbase.stock.temp.table.name");
		String kafkaZookeeperHosts = properties.getProperty("kafka.zookeeper.hosts");
		BrokerHosts kafkaBrokerHosts = new ZkHosts(kafkaZookeeperHosts);
		
		// ActiveMQ properties
		String activeMQUser = properties.getProperty("activemq.user", "admin");
        String activeMQPassword = properties.getProperty("activemq.password", "password");
        String activeMQHost = properties.getProperty("activemq.host","localhost");
        int activeMQPort = Integer.parseInt(properties.getProperty("activemq.port","61613")); 
        String activeMQDestination = properties.getProperty("activemq.topic","/topic/event");
        ActiveMQBoltConfig activeMQConfig = new ActiveMQBoltConfig(activeMQUser,activeMQPassword,activeMQHost,activeMQPort,activeMQDestination);
        
		SpoutConfig kafkaTransactionConfig = new SpoutConfig(kafkaBrokerHosts, kafkaTransactionTopic, "",
				"storm");
		SpoutConfig kafkaStockConfig = new SpoutConfig(kafkaBrokerHosts, kafkaStockTopic, "",
				"storm");
		kafkaTransactionConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaStockConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		
		SimpleHBaseMapper hBaseMapper = new SimpleHBaseMapper()
				.withRowKeyField("shop|product")
				.withCounterFields(new Fields("stock"))
				.withColumnFields(new Fields("order"))
				.withColumnFamily("Stock");
		
		//HBaseBolt hbaseBolt = new HBaseBolt(hbaseStockTable, hBaseMapper);
		HBaseBolt hbaseBolt = new HBaseBolt(hbaseStockTable, hBaseMapper, properties);
		
		JSONRetailParserBolt JSONParserBolt = new JSONRetailParserBolt();
		ProccesRetailTransactionBolt proccesTransactionBolt = new ProccesRetailTransactionBolt(hbaseStockTable,hbaseStockTempTable);
		ProccesRetailStockBolt proccesStockBolt = new ProccesRetailStockBolt();
		//TODO: Meterle este cuando haga el cliente de ActiveMQ
		//SendOrderProductBolt senOrderProductBolt = new SendOrderProductBolt(activeMQConfig);
		SendOrderProductToActiveMQBolt sendOrderProductBolt = new SendOrderProductToActiveMQBolt(activeMQConfig);
		
		builder.setSpout("KafkaTransactionSpout", new KafkaSpout(kafkaTransactionConfig), 1);
		builder.setSpout("KafkaStockSpout", new KafkaSpout(kafkaStockConfig), 1);

		builder.setBolt("JSONParserBolt", JSONParserBolt, 1).shuffleGrouping("KafkaTransactionSpout")
					.shuffleGrouping("KafkaStockSpout");
		
		builder.setBolt("ProccesTransactionBolt", proccesTransactionBolt, 1).shuffleGrouping("JSONParserBolt","transactionStream");
		builder.setBolt("ProccesStockBolt", proccesStockBolt, 1).shuffleGrouping("JSONParserBolt","stockStream");
		builder.setBolt("HBaseBolt", hbaseBolt, 1).fieldsGrouping("ProccesTransactionBolt", "hbaseStream",
				new Fields("shop|product")).fieldsGrouping("ProccesStockBolt", new Fields("shop|product"));
		builder.setBolt("SendOrderProductBolt", sendOrderProductBolt, 1).shuffleGrouping("ProccesTransactionBolt",
				"orderStream");
		
		return builder.createTopology();
	}
	
	private static List<String> parseZkHosts(String zkNodes) {

		String[] hostsAndPorts = zkNodes.split(",");
		List<String> hosts = new ArrayList<String>(hostsAndPorts.length);

		for (int i = 0; i < hostsAndPorts.length; i++) {
			hosts.add(i, hostsAndPorts[i].split(":")[0]);
		}
		return hosts;
	}

	private static int parseZkPort(String zkNodes) {

		String[] hostsAndPorts = zkNodes.split(",");
		int port = Integer.parseInt(hostsAndPorts[0].split(":")[1]);
		return port;
	}
	

	private static Properties loadProperties(String propertiesFile) throws Exception {
		Properties properties = new Properties();
		FileInputStream in = new FileInputStream(propertiesFile);
		properties.load(in);
		in.close();
		
		return properties;
	}
	
	public static void buildTopologyAndSubmit(String propertiesFile,
			Config config) throws Exception {
		
		Properties properties = loadProperties(propertiesFile);
		
		String stormExecutionMode = properties.getProperty("storm.execution.mode","local");
		int stormWorkersNumber = Integer.parseInt(properties.getProperty("storm.workers.number","2"));
		int maxTaskParallism = Integer.parseInt(properties.getProperty("storm.max.task.parallelism","2"));
		String topologyName = properties.getProperty("storm.topology.name","UnnamedTopology");
		String stormZookeeperHosts = properties.getProperty("storm.zookeeper.hosts");
		
		int topologyBatchEmitMillis = Integer.parseInt(
				properties.getProperty("storm.topology.batch.interval.miliseconds","2000"));
		
		// How often a batch can be emitted in a Trident topology.
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, topologyBatchEmitMillis);
		config.setNumWorkers(stormWorkersNumber);
		config.setMaxTaskParallelism(maxTaskParallism);
		
		/*
		Map<String,String> test = new HashMap<String,String>();
		test.put("hbase.cluster.distributed", "true");
		test.put("hbase.rootdir", "hdfs://openbus01:8020/hbase");
		test.put("hbase.zookeeper.quorum", "openbus01,openbus02,openbus03");
		
		config.put("hbase.config",test);
		*/
		
		StormTopology stormRetailTopology = buildTopology(properties);
	
		switch (stormExecutionMode){
			case ("cluster"):
				String nimbusHost = properties.getProperty("storm.nimbus.host","localhost");
				String nimbusPort = properties.getProperty("storm.nimbus.port","6627");
				config.put(Config.NIMBUS_HOST, nimbusHost);
				config.put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt(nimbusPort));
				config.put(Config.STORM_ZOOKEEPER_PORT, parseZkPort(stormZookeeperHosts));
				config.put(Config.STORM_ZOOKEEPER_SERVERS, parseZkHosts(stormZookeeperHosts));
				
				for (Entry e:config.entrySet()){
					System.out.println(e.getKey() + ": " + e.getValue());
				}
				StormSubmitter.submitTopology(topologyName, config, stormRetailTopology);
				break;
			case ("local"):
			default:
				int localTimeExecution = Integer.parseInt(properties.getProperty("storm.local.execution.time","20000"));
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyName, config, stormRetailTopology);
				Thread.sleep(localTimeExecution);
				cluster.killTopology(topologyName);
				cluster.shutdown();
				System.exit(0);
		}		
	}
}
