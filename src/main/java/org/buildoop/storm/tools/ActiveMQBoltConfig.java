package org.buildoop.storm.tools;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ActiveMQBoltConfig implements Serializable{
	
	private String activeMQUser,activeMQPassword,activeMQHost,activeMQDestination;

	private int activeMQPort;
	
	public ActiveMQBoltConfig(String activeMQUser, String activeMQPassword, String activeMQHost, int activeMQPort, String activeMQDestination){
		this.activeMQUser=activeMQUser;
		this.activeMQPassword=activeMQPassword;
		this.activeMQHost=activeMQHost;
		this.activeMQDestination=activeMQDestination;
		this.activeMQPort=activeMQPort;
	}
	
	public String getActiveMQUser() {
		return activeMQUser;
	}

	public String getActiveMQPassword() {
		return activeMQPassword;
	}

	public String getActiveMQHost() {
		return activeMQHost;
	}

	public String getActiveMQDestination() {
		return activeMQDestination;
	}

	public int getActiveMQPort() {
		return activeMQPort;
	}

}
