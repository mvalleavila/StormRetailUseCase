package org.buildoop.storm;

import backtype.storm.Config;

import org.buildoop.storm.tools.RetailTopologyTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetailTopology {
	public static final Logger LOG = LoggerFactory
			.getLogger(RetailTopology.class);


	public static void main(String[] args) throws Exception {
		
		String propertiesFile = args[0];
		Config config = new Config();
		RetailTopologyTools.buildTopologyAndSubmit(propertiesFile,config);
	}

}
