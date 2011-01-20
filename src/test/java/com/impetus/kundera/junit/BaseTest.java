package com.impetus.kundera.junit;

import java.io.IOException;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.cassandra.service.EmbeddedCassandraService;

import junit.framework.TestCase;

public abstract class BaseTest extends TestCase {

	/** The embedded server cassandra. */
    private static EmbeddedCassandraService cassandra;

	 protected void startCassandraServer () throws Exception {
	    	if (!checkIfServerRunning()) {
		        URL configURL = TestKundera.class.getClassLoader().getResource("storage-conf.xml");
		        try {
		            String storageConfigPath = configURL.getFile().substring(1).substring(0, configURL.getFile().lastIndexOf("/"));
		            System.setProperty("storage-config", storageConfigPath);
		        } catch (Exception e) {
		            fail("Could not find storage-config.xml sfile");
		        }
		        cassandra = new EmbeddedCassandraService();
		        cassandra.init();
		        Thread t = new Thread(cassandra);
		        t.setDaemon(true);
		        t.start();   	
	    	}
	    }
	    
	    private boolean checkIfServerRunning() {
			try {
				Socket socket = new Socket("127.0.0.1",9165);
				return socket.getInetAddress() !=null;
			} catch (UnknownHostException e) {
				return false;
			} catch (IOException e) {
				return false;
			}
		}
}
