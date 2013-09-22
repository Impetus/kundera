/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.impetus.kundera.ycsb.benchmark;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.graphdb.index.UniqueFactory;

import scala.annotation.target.field;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * Neo4J Native client for YCSB framework.
 * 
 * Properties to set:
 * 
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=normal
 * 
 * @author Amresh Singh
 * 
 */
public class Neo4JNativeClient extends DB
{
    private static GraphDatabaseService graphDb;
//    private GraphDatabaseService graphDb;
    Transaction tx = null;
    private int j;
    static {
        Map<String, String> config = new HashMap<String, String>(); 
        config.put("node_auto_indexing", "true");
        config.put("node_keys_indexable", "USER_ID");        
        
        
        String datastoreFilePath = "target/neo4jPerfNative.db";
        GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(datastoreFilePath);
        builder.setConfig(config);                
        
        graphDb = builder.newGraphDatabase();           
        
        System.out.println("Neo4J connection created with file path: " + datastoreFilePath);
        
        
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override    
    public void init() throws DBException
    {    
/*        Map<String, String> config = new HashMap<String, String>(); 
        config.put("node_auto_indexing", "true");
        config.put("node_keys_indexable", "USER_ID");        
        
        
        String datastoreFilePath = "target/neo4jPerfNative.db";
        GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(datastoreFilePath);
        builder.setConfig(config);                
        
        graphDb = builder.newGraphDatabase();           
        
*///        System.out.println("Neo4J connection created with file path: " + datastoreFilePath);
        tx = graphDb.beginTx();
        j = 1;
    }    
    

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override    
    public void cleanup() throws DBException
    {
        tx.success();
        tx.finish();
        /*try
        {
            //graphDb.shutdown();
        }
        catch (Exception e1)
        {
            System.err.println("Could not close Neo4J connection: " + e1.toString());
            e1.printStackTrace();
            return;
        }*/
    }
    
    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {
        try
        {            
            
            /*Node node = graphDb.createNode();
            node.setProperty("USER_ID", key);
            node.setProperty("NAME", key + "Keenu Reeves");
            node.setProperty("AGE", key + "39");
            node.setProperty("ADDRESS", key + "New Street"); */
            
            
            UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDb, "users")
            {
                @Override
                protected void initialize(Node created, Map<String, Object> properties)
                {
                    created.setProperty("USER_ID", properties.get("USER_ID"));
                }
            };           
           Node node = factory.getOrCreate("USER_ID", key);
           node.setProperty("NAME", key + "Keenu Reeves");
           node.setProperty("AGE", key + "39");
           node.setProperty("ADDRESS", key + "New Street");            
            
            j++;
            if (j % 1000 == 0)
            {
                tx.success();
                tx.finish();                
                tx = graphDb.beginTx();
            }
            
            
            return 0;           
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return 1;
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
    {
    	boolean present=false;
        try
        {            
            ReadableIndex<Node> autoNodeIndex = graphDb.index().getNodeAutoIndexer().getAutoIndex();
            IndexHits<Node> nodesFound = autoNodeIndex.get("USER_ID", key);
            Node node = null;
            
            assert nodesFound.size() == 1;
            
            if(nodesFound.hasNext())
            {
                node = nodesFound.next();
            }
            
            Iterable<String> fieldNames = node.getPropertyKeys();
            
            Iterator<String> iter = fieldNames.iterator();
            
            while(iter.hasNext())
            {
            	present=true;
            	String field = iter.next();
            	Object value = node.getProperty(field);
            	assert value != null;
            	assert field != null;
            }
        }
        catch (Exception e)
        {
            System.err.println(e.toString());
            return 1;
        }
        return present? 0:1;
    }
    
 
    
    
    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values)
    {
        return insert(table, key, values);
        /*com.mongodb.DB db = null;
        try
        {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject u = new BasicDBObject();
            DBObject fieldsToSet = new BasicDBObject();
            Iterator<String> keys = values.keySet().iterator();
            while (keys.hasNext())
            {
                String tmpKey = keys.next();
                fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());

            }
            u.put("$set", fieldsToSet);
            WriteResult res = collection.update(q, u, false, false, writeConcern);
            return res.getN() == 1 ? 0 : 1;
        }
        catch (Exception e)
        {
            System.err.println(e.toString());
            return 1;
        }
        finally
        {
            if (db != null)
            {
                db.requestDone();
            }
        }*/
//        return 0;
    }
    
    
    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key)
    {  
        Transaction tx = null;
        try
        {
            tx = graphDb.beginTx();
            
            Node node = graphDb.getNodeById(Long.parseLong(key));
            node.delete();
            
            tx.success();
            tx.finish();
            
            /*db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            WriteResult res = collection.remove(q, writeConcern);
            return res.getN() == 1 ? 0 : 1;*/
            
            return 0;
        }
        catch (Exception e)
        {
            System.err.println(e.toString());
            tx.failure();
            return 1;
        }        
    } 
   
    
    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    @SuppressWarnings("unchecked")
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result)
    {
        /*com.mongodb.DB db = null;
        try
        {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startkey);
            DBObject q = new BasicDBObject().append("_id", scanRange);
            DBCursor cursor = collection.find(q).limit(recordcount);
            while (cursor.hasNext())
            {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                result.add(StringByteIterator.getByteIteratorMap((Map<String, String>) cursor.next().toMap()));
            }

            return 0;
        }
        catch (Exception e)
        {
            System.err.println(e.toString());
            return 1;
        }
        finally
        {
            if (db != null)
            {
                db.requestDone();
            }
        }*/
        return 0;

    }
}
