/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.client.junit;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
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
    // private static EmbeddedCassandraService cassandra;

    /** The client. */
    private Cassandra.Client client;

    /** The logger. */
    private static Logger logger = Logger.getLogger(BaseTest.class);

    /**
     * Start cassandra server.
     *
     * @throws Exception
     *             the exception
     */
    protected void startCassandraServer() throws Exception
    {
        /*
         * if (!checkIfServerRunning()) { // cassandra = new
         * EmbeddedCassandraService(); // cassandra.start(); // startSolandra();
         *
         * }
         */
        initClient();
        logger.info("Loading Data:");
        loadData();
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
     */
    private void initClient()
    {
        TSocket socket = new TSocket("127.0.0.1", 9160);
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
     * Load data.
     *
     * @throws ConfigurationException
     *             the configuration exception
     * @throws TException
     *             the t exception
     * @throws NotFoundException
     *             the not found exception
     * @throws InvalidRequestException
     *             the invalid request exception
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
        timeLine_Def.setComparator_type("UTF8Type");
        timeLine_Def.setColumn_type("Super");
        timeLine_Def.setSubcomparator_type("UTF8Type");
        timeLine_Def.setDefault_validation_class("UTF8Type");
        CfDef users_Def = new CfDef("Blog", "users");
        users_Def.setComparator_type("UTF8Type");
        users_Def.setColumn_type("Super");
        users_Def.setSubcomparator_type("UTF8Type");
        users_Def.setDefault_validation_class("UTF8Type");
        CfDef preference_Def = new CfDef("Blog", "preference");
        CfDef external_Def = new CfDef("Blog", "externalLinks");
        CfDef imDetails_Def = new CfDef("Blog", "imDetails");
        //Added for snsUser Test.
        CfDef snsUser_Def = new CfDef("Blog", "snsusers");

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);
        cfDefs.add(userName_Def);
        cfDefs.add(friends_Def);
        cfDefs.add(followers_Def);
        cfDefs.add(tweet_Def);
        cfDefs.add(userLine_Def);
        cfDefs.add(timeLine_Def);
        cfDefs.add(users_Def);
        cfDefs.add(user_Def);
        cfDefs.add(preference_Def);
        cfDefs.add(external_Def);
        cfDefs.add(imDetails_Def);

        cfDefs.add(snsUser_Def);

        KsDef ksDef = new KsDef("Blog", simple.getCanonicalName(),  cfDefs);
        ksDef.setReplication_factor(1);
        
        client.send_system_add_keyspace(ksDef);
        logger.info("Data loaded");

    }

}
