/**
 * 
 */
package com.impetus.kundera.loader;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * @author impetus
 *
 */
public class ClientIdentifier {

	private String[] node;
	private int port;
	private String keyspace;
    private ClientType clientType;
    private String persistenceUnit;

	public ClientIdentifier(String[] contactNodes, int port, String keyspace, ClientType clientType, String persistenceUnit){
		this.node = contactNodes;
		this.port  = port;
		this.keyspace = keyspace;
		this.clientType = clientType;
		this.persistenceUnit = persistenceUnit;
	}
	
	
	public String[] getNode() {
		return node;
	}


	public int getPort() {
		return port;
	}


	public String getKeyspace() {
		return keyspace;
	}


	public ClientType getClientType() {
		return clientType;
	}


	/**
	 * @return the persistenceUnit
	 */
	public String getPersistenceUnit() {
		return persistenceUnit;
	}


	@Override
	public boolean equals(Object client){
		if(!(client instanceof ClientIdentifier)) {
			return false;
		} else {
			ClientIdentifier proxy = (ClientIdentifier) client;
			return proxy.getClientType().equals(this.clientType)
					&& proxy.getKeyspace().equals(this.getKeyspace())
					&& proxy.getPort()==(this.getPort())
					&& proxy.getNode().equals(this.getNode()) && proxy.getPersistenceUnit().equals(this.getPersistenceUnit());
		}
		
	}
	
	@Override
	public String toString(){
		return ToStringBuilder.reflectionToString(this);
	}
	
	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}
}
