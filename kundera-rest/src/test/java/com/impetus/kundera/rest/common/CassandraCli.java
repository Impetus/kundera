/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.rest.common;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * The Class CassandraCli.
 * 
 * @author vivek.mishra
 */
public final class CassandraCli
{

    /** The cassandra. */
    private static EmbeddedCassandraService cassandra;

    /** The client. */
    public static Cassandra.Client client;

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(CassandraCli.class);

    /** The tr. */
    private static TTransport tr;

    /**
     * Cassandra set up.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws TException
     *             the t exception
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws UnavailableException
     *             the unavailable exception
     * @throws TimedOutException
     *             the timed out exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    public static void cassandraSetUp() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {

        if (!checkIfServerRunning())
        {
            cassandra = new EmbeddedCassandraService();
            cassandra.start();
            initClient();
        }
    }

    /**
     * Create keyspace.
     * 
     * @param keyspaceName
     *            keyspace name.
     */
    public static void createKeySpace(String keyspaceName)
    {
        String nativeSql = "CREATE KEYSPACE " + keyspaceName
                + " with strategy_class = 'SimpleStrategy' and strategy_options:replication_factor=1";
        try
        {
            KsDef ks_Def = client.describe_keyspace(keyspaceName);
        }
        catch (NotFoundException e)
        {
            List<CfDef> cfDefs = new ArrayList<CfDef>();
            KsDef ks_Def = new KsDef(keyspaceName, SimpleStrategy.class.getName(), cfDefs);

            // Set replication factor
            if (ks_Def.strategy_options == null)
            {
                ks_Def.strategy_options = new LinkedHashMap<String, String>();
            }
            // Set replication factor, the value MUST be an integer
            ks_Def.strategy_options.put("replication_factor", "1");

            try
            {
                client.system_add_keyspace(ks_Def);
            }
            catch (TException e1)
            {
                log.error(e1);
            }
            catch (InvalidRequestException ess)
            {
                log.error(ess);
            }
            catch (SchemaDisagreementException sde)
            {
                log.error(sde);
            }

        }

        catch (InvalidRequestException e)
        {
            log.error(e);
        }
        catch (TException e)
        {
            log.error(e);
        }

    }

    /**
     * Drop out key space.
     * 
     * @param keyspaceName
     *            keyspace name
     */
    public static void dropKeySpace(String keyspaceName)
    {
        try
        {
            client.system_drop_keyspace(keyspaceName);
        }
        catch (InvalidRequestException e)
        {
            log.error(e);
        }
        catch (SchemaDisagreementException e)
        {
            log.error(e);
        }
        catch (TException e)
        {
            log.error(e);
        }

    }

    public static boolean keyspaceExist(String keySpaceName)
    {
        try
        {
            return client.describe_keyspace(keySpaceName) != null;
        }
        catch (NotFoundException e)
        {
            return false;
        }
        catch (InvalidRequestException e)
        {
            log.error(e);
        }
        catch (TException e)
        {
            log.error(e);
        }
        return false;
    }

    public static boolean columnFamilyExist(String columnfamilyName, String keyspaceName)
    {
        try
        {
            client.set_keyspace(keyspaceName);
            client.system_add_column_family(new CfDef(keyspaceName, columnfamilyName));
        }
        catch (InvalidRequestException e)
        {
            return true;
        }
        catch (SchemaDisagreementException e)
        {
            return false;
        }
        catch (TException e)
        {
            return false;
        }
        return false;
    }

    /**
     * Check if server running.
     * 
     * @return true, if successful
     */
    private static boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 9160);
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
     * 
     * @throws TTransportException
     *             the t transport exception
     */
    public static void initClient() throws TTransportException
    {
        TSocket socket = new TSocket("127.0.0.1", 9160);
        TTransport transport = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Cassandra.Client(protocol);
        socket.open();

    }

    /**
     * @return the client
     */
    public static Cassandra.Client getClient()
    {
        return client;
    }
    
    public static void truncateColumnFamily(String keyspace, String... columns)
    {
        try
        {
            if (client != null)
            {
                client.set_keyspace(keyspace);
                for (String column : columns)
                {
                    if (columnFamilyExist(column, keyspace))
                    {
                        client.truncate(column);
                    }
                }
            }
        }
        catch (IllegalArgumentException iex)
        {
            // do nothing.
        }
        catch (InvalidRequestException e)
        {
            // do nothing.
        }
        catch (UnavailableException e)
        {
            // do nothing.
        }
        catch (TimedOutException e)
        {
            // do nothing.
        }
        catch (TException e)
        {
            // do nothing.
        }
    }

}
