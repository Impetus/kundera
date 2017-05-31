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
package com.impetus.kundera.tests.crossdatastore.pickr;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.crud.RDBMSCli;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.pickr.dao.Pickr;
import com.impetus.kundera.tests.crossdatastore.pickr.dao.PickrImpl;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * @author amresh.singh
 */
public abstract class PickrBaseTest
{
    protected static final boolean RUN_IN_EMBEDDED_MODE = true;

    protected static final boolean AUTO_MANAGE_SCHEMA = true;

    protected Pickr pickr;

    protected int photographerId;

    protected static final String SCHEMA = "Pickr";

    protected String pu = "piccandra,picmysql,picongo,secIdxAddCassandra,addMongo";

    protected RDBMSCli cli;

    private static Logger log = LoggerFactory.getLogger(PickrBaseTest.class);

    protected void setUp() throws Exception
    {
        if (RUN_IN_EMBEDDED_MODE)
        {
            CassandraCli.cassandraSetUp();
            try
            {

                cli = new RDBMSCli(SCHEMA);

                cli.createSchema(SCHEMA);
            }
            catch (Exception e)
            {
                log.error("Error in RDBMS cli ", e);
            }
            // HBaseCli.startCluster();
        }

        if (AUTO_MANAGE_SCHEMA)
        {
            createCassandraSchema();
            createRDBMSTables();
        }

        photographerId = 1;
        pickr = new PickrImpl(pu);
    }

    private void createRDBMSTables() throws SQLException
    {
        try
        {
            cli.update("CREATE TABLE PICKR.PHOTOGRAPHER (PHOTOGRAPHER_ID INT PRIMARY KEY, PHOTOGRAPHER_NAME VARCHAR(256),ALBUM_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM PICKR.PHOTOGRAPHER");
//            cli.update("DROP TABLE PICKR.PHOTOGRAPHER");
//            cli.update("CREATE TABLE PICKR.PHOTOGRAPHER (PHOTOGRAPHER_ID INT PRIMARY KEY, PHOTOGRAPHER_NAME VARCHAR(256),ALBUM_ID VARCHAR(150))");
        }
        try
        {
            cli.update("CREATE TABLE PICKR.PHOTOGRAPHER_ALBUM (PHOTOGRAPHER_ID INT , ALBUM_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM PICKR.PHOTOGRAPHER_ALBUM");
//            cli.update("DROP TABLE PICKR.PHOTOGRAPHER_ALBUM");
//            cli.update("CREATE TABLE PICKR.PHOTOGRAPHER_ALBUM (PHOTOGRAPHER_ID INT , ALBUM_ID VARCHAR(150))");
        }

    }

    public void executeTests()
    {
        addPhotographer();
        getPhotographer();
        updatePhotographer();
        getAllPhotographers();
        deletePhotographer();
    }

    protected void tearDown() throws Exception
    {
        // pickr.close();
        if (AUTO_MANAGE_SCHEMA)
        {
            truncateSchema();
           // CassandraCli.dropKeySpace("Pickr");
            truncateMongo();

            deleteRDBMSSchemaAndTables();

        }

        if (RUN_IN_EMBEDDED_MODE)
        {
            if (cli != null)
            {
                cli.closeConnection();
            }
        }

    }

    protected void addKeyspace(KsDef ksDef, List<CfDef> cfDefs) throws InvalidRequestException,
            SchemaDisagreementException, TException
    {
        ksDef = new KsDef("Pickr", SimpleStrategy.class.getSimpleName(), cfDefs);
        // Set replication factor
        if (ksDef.strategy_options == null)
        {
            ksDef.strategy_options = new LinkedHashMap<String, String>();
        }
        // Set replication factor, the value MUST be an integer
        ksDef.strategy_options.put("replication_factor", "1");
        CassandraCli.client.system_add_keyspace(ksDef);
    }

    /**
     * 
     */
    private void truncateMongo()
    {
        String host = "localhost";
        String port = "27017";
        try
        {
            Mongo m = null;
            if (host != null && port != null)
            {
                m = new Mongo(host, Integer.parseInt(port));
                m.getDB("Pickr").dropDatabase();
            }
        }
        catch (NumberFormatException e)
        {
            log.error(e.getMessage());
        }
        catch (MongoException e)
        {
            log.error(e.getMessage());
        }
    }
    
    private void truncateSchema() throws InvalidRequestException, SchemaDisagreementException
    {
        log.warn("Truncating....");

        
        CassandraCli.truncateColumnFamily("Pickr", "PHOTOGRAPHER","ALBUM", "PHOTO", "ALBUM_PHOTO");
    //    CassandraCli.dropColumnFamily("Pickr", "MOVIE");
       
    }

    private void deleteRDBMSSchemaAndTables()
    {

        try
        {
            cli.update("DELETE FROM PICKR.PHOTOGRAPHER");
         //   cli.update("DROP TABLE PICKR.PHOTOGRAPHER");
            cli.update("DELETE FROM PICKR.PHOTOGRAPHER_ALBUM");
        //    cli.update("DROP TABLE PICKR.PHOTOGRAPHER_ALBUM");
          //  cli.dropSchema(SCHEMA);
            
            
        }
        catch (SQLException e)
        {
            if (cli != null && RUN_IN_EMBEDDED_MODE)
            {
                try
                {
                    cli.closeConnection();
                }
                catch (SQLException e1)
                {
                	log.warn(e1.getMessage());
                }
            }
        }

    }

    protected abstract void addPhotographer();

    protected abstract void updatePhotographer();

    protected abstract void getPhotographer();

    protected abstract void getAllPhotographers();

    protected abstract void deletePhotographer();

    protected abstract void createCassandraSchema() throws IOException, TException, InvalidRequestException,
            UnavailableException, TimedOutException, SchemaDisagreementException;
}
