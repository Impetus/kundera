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
import lucandra.CassandraUtils;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * The Class BaseTest.
 */
public abstract class BaseTest extends TestCase
{

    /** The embedded server cassandra. */
    private static EmbeddedCassandraService cassandra;

    /** The client. */
    private Cassandra.Client client;
    
    private static Logger logger =  Logger.getLogger(BaseTest.class);

    /**
     * Start cassandra server.
     *
     * @throws Exception the exception
     */
    protected void startCassandraServer() throws Exception
    {
        if (!checkIfServerRunning())
        {
            cassandra = new EmbeddedCassandraService();
            cassandra.start();
            startSolandra();
            initClient();
            loadData();

        }
    }

    /**
     * Check if server running.
     *
     * @return true, if successful
     */
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

    /**
     * Inits the client.
     */
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
        catch (TTransportException ttex)
        {
            logger.error(ttex.getMessage());
        }
        catch (Exception ex)
        {
            logger.error(ex.getMessage());
        }

    }

    /**
     * Standard cfmd.
     *
     * @param ksName the ks name
     * @param cfName the cf name
     * @param columnType the column type
     * @return the cF meta data
     */
    private static CFMetaData standardCFMD(String ksName, String cfName, ColumnFamilyType columnType)
    {
        return new CFMetaData(ksName, cfName, columnType, UTF8Type.instance, null, "colfamily", Double.valueOf("0"),
                Double.valueOf("0"), Double.valueOf("0"), 0, UTF8Type.instance, 0, 0, 0, 0, 0, Integer.valueOf(0),
                Double.valueOf("0"), new HashMap<ByteBuffer, ColumnDefinition>());
    }

    /**
     * Load data.
     *
     * @throws ConfigurationException the configuration exception
     * @throws TException the t exception
     * @throws NotFoundException the not found exception
     * @throws InvalidRequestException the invalid request exception
     */
    private void loadData() throws org.apache.cassandra.config.ConfigurationException, TException, NotFoundException,
            InvalidRequestException
    {

        Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;
        Map<String, String> ret = new HashMap<String, String>();
        ret.put("replication_factor", "1");
        CfDef user_Def = new CfDef("Blog", "Person");
        CfDef userName_Def = new CfDef("Blog", "Department");
        CfDef friends_Def = new CfDef("Blog", "Employee");
        CfDef followers_Def = new CfDef("Blog", "Profile");
        CfDef tweet_Def = new CfDef("Blog", "Addresses");
        CfDef userLine_Def = new CfDef("Blog", "Authors");
        CfDef timeLine_Def = new CfDef("Blog", "Posts");
        CfDef users_Def = new CfDef("Blog", "users");
        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);
        cfDefs.add(userName_Def);
        cfDefs.add(friends_Def);
        cfDefs.add(followers_Def);
        cfDefs.add(tweet_Def);
        cfDefs.add(userLine_Def);
        cfDefs.add(timeLine_Def);
        cfDefs.add(users_Def);        

        client.send_system_add_keyspace(new KsDef("Blog", simple.getCanonicalName(), 1, cfDefs));

        KSMetaData metadata = new KSMetaData("Blog", simple, ret, 1, standardCFMD("Blog", "Person",
                ColumnFamilyType.Standard), standardCFMD("Blog", "Department", ColumnFamilyType.Standard),
                standardCFMD("Blog", "Employee", ColumnFamilyType.Standard), standardCFMD("Blog", "Profile",
                        ColumnFamilyType.Standard), standardCFMD("Blog", "Addresses", ColumnFamilyType.Standard),
                standardCFMD("Blog", "Authors", ColumnFamilyType.Standard), standardCFMD("Blog", "Posts",
                        ColumnFamilyType.Super), standardCFMD("Blog", "users", ColumnFamilyType.Super));
        for (CFMetaData cfm : metadata.cfMetaData().values())
        {
            CFMetaData.map(cfm);
        }

        DatabaseDescriptor.setTableDefinition(metadata, DatabaseDescriptor.getDefsVersion());
    }

    /**
     * Start solandra.
     */
    private void startSolandra()
    {

        CassandraUtils.cacheInvalidationInterval = 0; // real-time

        try
        {
            // Load solandra specific schema
            CassandraUtils.setStartup();
            CassandraUtils.createCassandraSchema();
        }
        catch (Throwable t)
        {
            logger.error("errror while starting solandra schema:", t);
        }

    }

}
