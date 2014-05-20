/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.tests.persistence.lazy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * Utilities for Test setup
 * 
 * @author amresh.singh
 */
public class LazyTestSetup
{
    private static final String COLUMN_FAMILY_PHOTOGRAPHER = "PHOTOGRAPHER_LAZY";

    private static final String COLUMN_FAMILY_ALBUM = "ALBUM_LAZY";

    private static final String KEYSPACE = "Pickr";

    public static final boolean RUN_IN_EMBEDDED_MODE = true;

    public static final boolean AUTO_MANAGE_SCHEMA = true;

    void startServer()
    {
        if (RUN_IN_EMBEDDED_MODE)
        {
            try
            {
                CassandraCli.cassandraSetUp();
            }
            catch (IOException e)
            {

            }
            catch (TException e)
            {

            }
        }

    }

    void stopServer()
    {
    }

    void createSchema()
    {
        if (AUTO_MANAGE_SCHEMA)
        {
            try
            {
                KsDef ksDef = null;

                CfDef cfDefPhotographer = new CfDef();
                cfDefPhotographer.name = COLUMN_FAMILY_PHOTOGRAPHER;
                cfDefPhotographer.keyspace = KEYSPACE;
                cfDefPhotographer.setKey_validation_class("Int32Type");
                cfDefPhotographer.setComparator_type("UTF8Type");

                ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("PHOTOGRAPHER_NAME".getBytes()), "UTF8Type");
                columnDef2.index_type = IndexType.KEYS;
                cfDefPhotographer.addToColumn_metadata(columnDef2);

                ColumnDef columnDef3 = new ColumnDef(ByteBuffer.wrap("ALBUM_ID".getBytes()), "UTF8Type");
                columnDef3.index_type = IndexType.KEYS;
                cfDefPhotographer.addToColumn_metadata(columnDef3);

                CfDef cfDefAlbum = new CfDef();
                cfDefAlbum.name = COLUMN_FAMILY_ALBUM;
                cfDefAlbum.keyspace = KEYSPACE;
                cfDefAlbum.setKey_validation_class("UTF8Type");
                cfDefAlbum.setComparator_type("UTF8Type");
                ColumnDef columnDef4 = new ColumnDef(ByteBuffer.wrap("ALBUM_NAME".getBytes()), "UTF8Type");
                columnDef4.index_type = IndexType.KEYS;
                cfDefAlbum.addToColumn_metadata(columnDef4);

                ColumnDef columnDef5 = new ColumnDef(ByteBuffer.wrap("ALBUM_DESC".getBytes()), "UTF8Type");
                columnDef5.index_type = IndexType.KEYS;
                cfDefAlbum.addToColumn_metadata(columnDef5);

                List<CfDef> cfDefs = new ArrayList<CfDef>();
                cfDefs.add(cfDefPhotographer);
                cfDefs.add(cfDefAlbum);

                try
                {
                    CassandraCli.initClient();
                    ksDef = CassandraCli.client.describe_keyspace(KEYSPACE);
                    CassandraCli.client.set_keyspace(KEYSPACE);

                    if (!CassandraCli.columnFamilyExist(COLUMN_FAMILY_PHOTOGRAPHER, KEYSPACE))
                    {
                        CassandraCli.client.system_add_column_family(cfDefPhotographer);
                    }
                    else
                    {
                        CassandraCli.truncateColumnFamily(KEYSPACE, COLUMN_FAMILY_PHOTOGRAPHER);
                    }

                    if (!CassandraCli.columnFamilyExist(COLUMN_FAMILY_ALBUM, KEYSPACE))
                    {
                        CassandraCli.client.system_add_column_family(cfDefAlbum);
                    }
                    else
                    {
                        CassandraCli.truncateColumnFamily(KEYSPACE, COLUMN_FAMILY_ALBUM);
                    }

                }
                catch (NotFoundException e)
                {

                    ksDef = new KsDef(KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
                    // Set replication factor
                    if (ksDef.strategy_options == null)
                    {
                        ksDef.strategy_options = new LinkedHashMap<String, String>();
                    }
                    // Set replication factor, the value MUST be an integer
                    ksDef.strategy_options.put("replication_factor", "1");
                    CassandraCli.client.system_add_keyspace(ksDef);
                }

                CassandraCli.client.set_keyspace(KEYSPACE);
            }
            catch (TException e)
            {

            }

        }

    }

    void deleteSchema()
    {
        if (AUTO_MANAGE_SCHEMA)
        {
            // CassandraCli.dropKeySpace(KEYSPACE);
            CassandraCli.truncateColumnFamily(KEYSPACE, COLUMN_FAMILY_PHOTOGRAPHER, COLUMN_FAMILY_ALBUM);
        }

    }

}
