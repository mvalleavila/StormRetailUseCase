package org.buildoop.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.buildoop.storm.bolts.JSONRetailParserBolt;
import org.buildoop.storm.bolts.ProccesRetailStockBolt;
import org.buildoop.storm.bolts.ProccesRetailTransactionBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class StormRetailTopology {
	public static final Logger LOG = LoggerFactory
			.getLogger(StormRetailTopology.class);

	private final BrokerHosts kafkaBrokerHosts;

	public StormRetailTopology(String zookeeperHosts) {
		kafkaBrokerHosts = new ZkHosts(zookeeperHosts);
	}

	public StormTopology buildTopology(Properties properties) {
		
		// Load properties for the storm topoology
		String kafkaTransactionTopic = properties.getProperty("kafka.transaction.topic");
		String kafkaStockTopic = properties.getProperty("kafka.stock.topic");
		String hbaseTable = properties.getProperty("hbase.table.name");
		String hbaseColumnFamily = properties.getProperty("hbase.column.family");
		
		SpoutConfig kafkaTransactionConfig = new SpoutConfig(kafkaBrokerHosts, kafkaTransactionTopic, "",
				"storm");
		SpoutConfig kafkaStockConfig = new SpoutConfig(kafkaBrokerHosts, kafkaStockTopic, "",
				"storm");
		kafkaTransactionConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaStockConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		
		SimpleHBaseMapper hBaseMapper = new SimpleHBaseMapper()
				.withRowKeyField("store|product")
				.withCounterFields(new Fields("quantity"))
				.withColumnFields(new Fields("sent"))
				.withColumnFamily(hbaseColumnFamily);
		
		HBaseBolt hbaseBolt = new HBaseBolt(hbaseTable, hBaseMapper);
		JSONRetailParserBolt JSONParserBolt = new JSONRetailParserBolt();
		ProccesRetailTransactionBolt proccesTransactionBolt = new ProccesRetailTransactionBolt(hbaseTable);
		ProccesRetailStockBolt proccesStockBolt = new ProccesRetailStockBolt(hbaseTable);
		

		builder.setSpout("KafkaTransactionSpout", new KafkaSpout(kafkaTransactionConfig), 1);
		builder.setSpout("KafkaStockSpout", new KafkaSpout(kafkaStockConfig), 1);
		builder.setBolt("JSONParserBolt", JSONParserBolt, 1).shuffleGrouping("KafkaTransactionSpout");
		builder.setBolt("JSONParserBolt", JSONParserBolt, 1).shuffleGrouping("KafkaStockSpout");
		
		builder.setBolt("ProccesTransactionBolt", proccesTransactionBolt, 1).shuffleGrouping("ParseBolt","transaction");
		builder.setBolt("ProccesStockBolt", proccesStockBolt, 1).shuffleGrouping("ParseBolt","stock");
		builder.setBolt("HBaseBolt", hbaseBolt, 1).fieldsGrouping("CountBolt",
				new Fields("store|product"));

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
	
	private static void loadTopologyPropertiesAndSubmit(Properties properties,
			Config config) throws Exception {
		
		String stormExecutionMode = properties.getProperty("storm.execution.mode","local");
		int stormWorkersNumber = Integer.parseInt(properties.getProperty("storm.workers.number","2"));
		int maxTaskParallism = Integer.parseInt(properties.getProperty("storm.max.task.parallelism","2"));
		String topologyName = properties.getProperty("storm.topology.name","AuditActiveLoginsCount");
		String zookeeperHosts = properties.getProperty("zookeeper.hosts");
		int topologyBatchEmitMillis = Integer.parseInt(
				properties.getProperty("storm.topology.batch.interval.miliseconds","2000"));
		
		// How often a batch can be emitted in a Trident topology.
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, topologyBatchEmitMillis);
		config.setNumWorkers(stormWorkersNumber);
		config.setMaxTaskParallelism(maxTaskParallism);
		
		StormRetailTopology auditActiveLoginsTopology = new StormRetailTopology(zookeeperHosts);
		StormTopology stormTopology = auditActiveLoginsTopology.buildTopology(properties);
	
		switch (stormExecutionMode){
			case ("cluster"):
				String nimbusHost = properties.getProperty("storm.nimbus.host","localhost");
				String nimbusPort = properties.getProperty("storm.nimbus.port","6627");
				config.put(Config.NIMBUS_HOST, nimbusHost);
				config.put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt(nimbusPort));
				config.put(Config.STORM_ZOOKEEPER_PORT, parseZkPort(zookeeperHosts));
				config.put(Config.STORM_ZOOKEEPER_SERVERS, parseZkHosts(zookeeperHosts));
				StormSubmitter.submitTopology(topologyName, config, stormTopology);
				break;
			case ("local"):
			default:
				int localTimeExecution = Integer.parseInt(properties.getProperty("storm.local.execution.time","20000"));
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyName, config, stormTopology);
				Thread.sleep(localTimeExecution);
				cluster.killTopology(topologyName);
				cluster.shutdown();
				System.exit(0);
		}		
	}

	public static void main(String[] args) throws Exception {
		
		String propertiesFile = args[0];
		Properties properties = loadProperties(propertiesFile);
		Config config = new Config();
	
		loadTopologyPropertiesAndSubmit(properties,config);

	}

}
