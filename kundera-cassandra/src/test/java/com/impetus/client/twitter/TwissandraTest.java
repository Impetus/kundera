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
package com.impetus.client.twitter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.databene.contiperf.PerfTest;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.report.CSVSummaryReportModule;
import org.databene.contiperf.report.HtmlReportModule;
import org.databene.contiperf.report.ReportModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.Constants;

/**
 * Test case for Cassandra.
 * 
 * @author amresh.singh
 */
public class TwissandraTest extends TwitterTestBaseCassandra
{

    /** The Constant LOG. */
    private static final Log log = LogFactory.getLog(TwissandraTest.class);

    @Rule
    public ContiPerfRule i = new ContiPerfRule(new ReportModule[] { new CSVSummaryReportModule(),
            new HtmlReportModule() });

    @Before
    public void setUp() throws Exception
    {
        setUpInternal(persistenceUnit);
    }

    @Test
    @PerfTest(invocations = 1000)
    public void onExecute() throws Exception
    {
        executeTwissandraTest();
    }

    @After
    public void tearDown() throws Exception
    {
        tearDownInternal();
    }

    @Override
    void startServer()
    {
        try
        {
            CassandraCli.cassandraSetUp();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (TException e)
        {
            e.printStackTrace();
        }
        catch (InvalidRequestException e)
        {
            e.printStackTrace();
        }
        catch (UnavailableException e)
        {
            e.printStackTrace();
        }
        catch (TimedOutException e)
        {
            e.printStackTrace();
        }
        catch (SchemaDisagreementException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    void stopServer()
    {
    }

    @Override
    void createSchema() throws InvalidRequestException, SchemaDisagreementException, TException
    {
        KsDef ksDef = null;

        CfDef userCfDef = new CfDef();
        userCfDef.name = "USER";
        userCfDef.keyspace = keyspace;
        userCfDef.column_type = "Super";
        userCfDef.setComparator_type("UTF8Type");
        userCfDef.setSubcomparator_type("AsciiType");
        userCfDef.setKey_validation_class("UTF8Type");

        CfDef userIndexCfDef = new CfDef();
        userIndexCfDef.name = "USER" + Constants.INDEX_TABLE_SUFFIX;
        userIndexCfDef.column_type = "Super";
        userIndexCfDef.keyspace = keyspace;
        userCfDef.setKey_validation_class("AsciiType");

        CfDef prefrenceCfDef = new CfDef();
        prefrenceCfDef.name = "PREFERENCE";
        prefrenceCfDef.keyspace = keyspace;
        prefrenceCfDef.setComparator_type("UTF8Type");
        prefrenceCfDef.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("WEBSITE_THEME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        ColumnDef columnDef3 = new ColumnDef(ByteBuffer.wrap("PRIVACY_LEVEL".getBytes()), "UTF8Type");
        columnDef3.index_type = IndexType.KEYS;
        prefrenceCfDef.addToColumn_metadata(columnDef);
        prefrenceCfDef.addToColumn_metadata(columnDef3);

        CfDef externalLinkCfDef = new CfDef();
        externalLinkCfDef.name = "EXTERNAL_LINK";
        externalLinkCfDef.keyspace = keyspace;
        externalLinkCfDef.setComparator_type("UTF8Type");
        externalLinkCfDef.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("LINK_TYPE".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("LINK_ADDRESS".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        ColumnDef columnDef4 = new ColumnDef(ByteBuffer.wrap("USER_ID".getBytes()), "UTF8Type");
        columnDef4.index_type = IndexType.KEYS;
        externalLinkCfDef.addToColumn_metadata(columnDef1);
        externalLinkCfDef.addToColumn_metadata(columnDef2);
        externalLinkCfDef.addToColumn_metadata(columnDef4);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(userCfDef);
        cfDefs.add(userIndexCfDef);
        cfDefs.add(prefrenceCfDef);
        cfDefs.add(externalLinkCfDef);
        try
        {
            ksDef = CassandraCli.client.describe_keyspace(keyspace);
            CassandraCli.client.set_keyspace(keyspace);
            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("USER"))
                {
                    CassandraCli.client.system_drop_column_family("USER");
                }
                if (cfDef1.getName().equalsIgnoreCase("USER" + Constants.INDEX_TABLE_SUFFIX))
                {
                    CassandraCli.client.system_drop_column_family("USER" + Constants.INDEX_TABLE_SUFFIX);
                }
                if (cfDef1.getName().equalsIgnoreCase("PREFERENCE"))
                {
                    CassandraCli.client.system_drop_column_family("PREFERENCE");
                }
                if (cfDef1.getName().equalsIgnoreCase("EXTERNAL_LINK"))
                {
                    CassandraCli.client.system_drop_column_family("EXTERNAL_LINK");
                }
            }
            CassandraCli.client.system_add_column_family(userCfDef);
            CassandraCli.client.system_add_column_family(userIndexCfDef);
            CassandraCli.client.system_add_column_family(externalLinkCfDef);
            CassandraCli.client.system_add_column_family(prefrenceCfDef);
        }
        catch (NotFoundException e)
        {
            ksDef = new KsDef(keyspace, SimpleStrategy.class.getSimpleName(), cfDefs);

            // Set replication factor
            if (ksDef.strategy_options == null)
            {
                ksDef.strategy_options = new LinkedHashMap<String, String>();
            }
            // Set replication factor, the value MUST be an integer
            ksDef.strategy_options.put("replication_factor", "1");

            CassandraCli.client.system_add_keyspace(ksDef);
        }
        catch (InvalidRequestException e)
        {
            log.error(e.getMessage());
        }
        catch (TException e)
        {
            log.error(e.getMessage());
        }
    }

    @Override
    void deleteSchema()
    {
        /*
         * LOG.warn(
         * "Truncating Column families and finally dropping Keyspace KunderaExamples in Cassandra...."
         * ); CassandraCli.dropColumnFamily("USER", keyspace);
         * CassandraCli.dropColumnFamily("PREFERENCE", keyspace);
         * CassandraCli.dropColumnFamily("EXTERNAL_LINKS", keyspace);
         * CassandraCli.dropKeySpace(keyspace);
         */
    }

}
