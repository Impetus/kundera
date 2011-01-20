/**
 * 
 */
package com.impetus.kundera.loader;

import java.util.HashMap;
import java.util.Map;

import com.impetus.kundera.Client;

/**
 * @author impetus
 */
public final class ClientResolver {
	
	static Map<ClientIdentifier, Client> clientsNew = new HashMap<ClientIdentifier, Client>();

	/**
	 *   
	 * @param clientIdentifier
	 * @return
	 */
	public static Client getClient(ClientIdentifier clientIdentifier) {
		if(clientsNew.containsKey(clientIdentifier)) {
			return clientsNew.get(clientIdentifier);
		}else {
			return loadNewProxyInstance(clientIdentifier);
		}
	}

	private static Client loadNewProxyInstance(ClientIdentifier clientIdentifier) {
		Client proxy = null;
	try {
			if (clientIdentifier.getClientType().equals(ClientType.HBASE)) {
				proxy = (Client) Class.forName(
						"com.impetus.kundera.hbase.client.HBaseClient")
						.newInstance();
			} else if (clientIdentifier.getClientType().equals(
					ClientType.PELOPS)) {
				proxy = (Client) Class.forName(
						"com.impetus.kundera.client.PelopsClient")
						.newInstance();
			} else if (clientIdentifier.getClientType().equals(
					ClientType.THRIFT)) {
				proxy = (Client) Class.forName(
						"com.impetus.kundera.client.ThriftClient")
						.newInstance();
			}
		} catch (InstantiationException e) {
				throw new ClientResolverException(e.getMessage());
			} catch (IllegalAccessException e) {
				throw new ClientResolverException(e.getMessage());
			} catch (ClassNotFoundException e) {
				 throw new ClientResolverException(e.getMessage());
			}
			
     		if(proxy !=null) {
     			proxy.setContactNodes(clientIdentifier.getNode());
     			proxy.setDefaultPort(clientIdentifier.getPort());
     			proxy.setKeySpace(clientIdentifier.getKeyspace());
     			clientsNew.put(clientIdentifier, proxy);
     		}else{
     			throw new ClientResolverException("No client configured:");
     		}
		return proxy;
	}
}
