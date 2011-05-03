package com.impetus.kundera.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.management.RuntimeErrorException;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.hbase.admin.DataHandler;
import com.impetus.kundera.hbase.admin.HBaseDataHandler;
import com.impetus.kundera.loader.DBType;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * HBase client.
 * @author impetus
 */
public class HBaseClient implements/* Client, */com.impetus.kundera.Client {
	String contactNode;
	String defaultPort;
	 private DataHandler handler /*= new HBaseDataHandler("localhost","6000")*/;
	 private boolean isConnected;
	 
	 private EntityManager em;

	/*@Override
	public void write(String tableName, String columnFamily, String rowKey, List<Column> columns, EnhancedEntity e)  throws IOException{
		handler.loadData(tableName, columnFamily, rowKey, columns,e);
	}

	@Override
	public HBaseData read(String tableName, String columnFamily, String rowKey, String...columnNames)  throws IOException{
		return handler.populateData(tableName, columnFamily, columnNames, rowKey);

	}*/

	@Override
	public void writeColumns(String keyspace, String columnFamily, String rowKey, List<Column> columns, EnhancedEntity e) throws Exception {
//		em.persist(arg0)
		handler.loadData(e.getEntity().getClass().getSimpleName().toLowerCase(), columnFamily, rowKey, columns,e);
	}
	
	@Override
	public void writeColumns(EntityManagerImpl em, EnhancedEntity e, EntityMetadata m) throws Exception {
		throw new PersistenceException("Not yet implemented");
	}


	@Override
	public <E> E loadColumns(EntityManagerImpl em, Class<E> clazz, String keyspace, String columnFamily, String rowKey, EntityMetadata m)	throws Exception {
		HBaseData data =		 handler.populateData(clazz.getSimpleName().toLowerCase(), columnFamily, new String[0], rowKey);
		 return onLoadFromHBase(clazz, data, m, rowKey);
	}

	@Override
	public <E> List<E> loadColumns(EntityManagerImpl em, Class<E> clazz,	String keyspace, String columnFamily, EntityMetadata m, String... keys) throws Exception {
		List<E> entities = new ArrayList<E>();
		for(String rowKey: keys) {
			HBaseData data =		handler.populateData(clazz.getSimpleName().toLowerCase(), columnFamily, keys, rowKey);
			  entities.add(onLoadFromHBase(clazz, data, m, rowKey));
		}
		return entities;
	}


	/**
	 * 
	 * @param <E>
	 * @param clazz
	 * @param data
	 * @param m
	 * @param columnFamily
	 * @param id
	 * @return
	 */
	private <E> E onLoadFromHBase(Class<E> clazz, HBaseData data, EntityMetadata m, String id){
		// Instantiate a new instance
		E e=null;
		try {
			e = clazz.newInstance();
			String colName = null;
			byte[] columnValue=null;
			PropertyAccessorHelper.set(e, m.getIdProperty(), id);
			List<KeyValue> values = data.getColumns();
			for(KeyValue colData:values){
				colName = Bytes.toString(colData.getQualifier());
				columnValue = colData.getValue();
				
				//Get Column from metadata
				com.impetus.kundera.metadata.EntityMetadata.Column column = m.getColumn(colName);
				PropertyAccessorHelper.set(e, column.getField(), columnValue);	
			}
		} catch (InstantiationException e1) {
			throw new RuntimeException(e1.getMessage());
		} catch (IllegalAccessException e1) {
			throw new RuntimeException(e1.getMessage());
		} catch (PropertyAccessException e1) {
			throw new RuntimeException(e1.getMessage());
		}
		
		return e;
	}

	@Override
	public void shutdown() {
		handler.shutdown();
	}

	@Override
	public void connect() {
		if(!isConnected) {
			handler = new HBaseDataHandler(contactNode,defaultPort);
			isConnected=true;
		}
	}

	@Override
	public void setContactNodes(String... contactNodes) {
		this.contactNode = contactNodes[0];
	}

	@Override
	public void setDefaultPort(int defaultPort) {
		this.defaultPort = String.valueOf(defaultPort);
	}

	@Override
	public void delete(String keyspace, String columnFamily, String rowId) throws Exception {
		throw new RuntimeException("TODO:not yet supprot");
		
	}

	@Override
	public void setKeySpace(String keySpace) {
		//TODO not required.
	}

	@Override
	public DBType getType() {
		return DBType.HBASE;
	}
}
