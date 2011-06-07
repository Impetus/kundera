package com.impetus.kundera.junit;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public abstract class BaseTest extends TestCase
{

    /** The embedded server cassandra. */
    private static EmbeddedCassandraService cassandra;
    
    private Cassandra.Client client;

    protected void startCassandraServer() throws Exception
    {
        if (!checkIfServerRunning())
        {
            cassandra = new EmbeddedCassandraService();
            cassandra.start();
        }
        initClient();
        loadData();
    }

    private boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 9165);
            return socket.getInetAddress() != null;
        }
        catch (UnknownHostException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }
    }
    
    private void initClient()
    {
        TSocket socket = new TSocket("127.0.0.1", 9165);
        TTransport transport = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Cassandra.Client(protocol);

        try
        {
            socket.open();
        }
        catch (TTransportException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch(Exception ex) 
        {
            ex.printStackTrace();
        }
        
       
    
    }
    
    private static CFMetaData standardCFMD(String ksName, String cfName, ColumnFamilyType columnType)
    {
        /**
         * String tableName, String cfName, ColumnFamilyType cfType,
         *  AbstractType comparator, AbstractType subcolumnComparator,
         *  String comment, double rowCacheSize, double keyCacheSize, 
         *  double readRepairChance, int gcGraceSeconds, AbstractType defaultValidator, 
         *  int minCompactionThreshold, int maxCompactionThreshold, int rowCacheSavePeriodInSeconds, 
         *  int keyCacheSavePeriodInSeconds, int memTime, Integer memSize, Double memOps, 
         *  Map<ByteBuffer, ColumnDefinition> column_metadata
         */
        return new CFMetaData(ksName, cfName, columnType, UTF8Type.instance, null,"colfamily",
                              Double.valueOf("0"),Double.valueOf("0"),Double.valueOf("0"),0,
                              UTF8Type.instance,0,0,0,0,0,Integer.valueOf(0),Double.valueOf("0"),new HashMap<ByteBuffer, ColumnDefinition>());
    }
    
    private  void loadData() throws org.apache.cassandra.config.ConfigurationException, TException, NotFoundException, InvalidRequestException 
    {
       
        Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;
        Map<String, String> ret = new HashMap<String,String>();
        ret.put("replication_factor", "1");
        CfDef user_Def = new CfDef("Blog", "Person");
        CfDef userName_Def = new CfDef("Blog", "Department");
        CfDef friends_Def = new CfDef("Blog", "Employee");
        CfDef followers_Def = new CfDef("Blog", "Profile");
        CfDef tweet_Def = new CfDef("Blog", "Addresses");
        CfDef userLine_Def = new CfDef("Blog", "Authors");
        CfDef timeLine_Def = new CfDef("Blog", "Posts");
        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);
        cfDefs.add(userName_Def);
        cfDefs.add(friends_Def);
        cfDefs.add(followers_Def);
        cfDefs.add(tweet_Def);
        cfDefs.add(userLine_Def);
        cfDefs.add(timeLine_Def);
        
        client.send_system_add_keyspace(new KsDef("Blog", simple.getCanonicalName(),1, cfDefs));
        
        KSMetaData metadata = new KSMetaData("Blog", simple, ret,1, standardCFMD("Blog", "Person", ColumnFamilyType.Standard),
                                                                           standardCFMD("Blog", "Department", ColumnFamilyType.Standard),
                                                                           standardCFMD("Blog", "Employee", ColumnFamilyType.Standard),
                                                                           standardCFMD("Blog", "Profile", ColumnFamilyType.Standard),
                                                                           standardCFMD("Blog", "Addresses", ColumnFamilyType.Standard),
                                                                           standardCFMD("Blog", "Authors", ColumnFamilyType.Standard),
                                                                           standardCFMD("Blog", "Posts", ColumnFamilyType.Super));
        for (CFMetaData cfm : metadata.cfMetaData().values())
        {
            CFMetaData.map(cfm);
        }
          
        DatabaseDescriptor.setTableDefinition(metadata, DatabaseDescriptor.getDefsVersion());
    }
    
    
}
