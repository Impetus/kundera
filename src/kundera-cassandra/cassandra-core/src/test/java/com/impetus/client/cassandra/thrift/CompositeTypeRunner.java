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
package com.impetus.client.cassandra.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CompositeType.Builder;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

/**
 * @author vivek.mishra
 * 
 */
public class CompositeTypeRunner
{
    public static void main(String[] args) throws Exception
    {

        TSocket socket = new TSocket("localhost", 9160);
        TFramedTransport transport = new TFramedTransport(socket);

        Cassandra.Client client = new Cassandra.Client(new TBinaryProtocol(transport, true, true));
        transport.open();
        client.set_cql_version("3.0.0");
        List<CfDef> cfDefs = new ArrayList<CfDef>();

        /*
         * CfDef cfDef = new CfDef(); cfDef.setName("test"); cfDef.keyspace =
         * "bigdata"; cfDef.setComparator_type("UTF8Type");
         * cfDef.setDefault_validation_class("UTF8Type"); //
         * cfDef.setKey_validation_class("UTF8Type");
         * 
         * cfDefs.add(cfDef);
         */

        KsDef ksDef = new KsDef("bigdata", SimpleStrategy.class.getName(), cfDefs);

        if (ksDef.strategy_options == null)
        {
            ksDef.strategy_options = new LinkedHashMap<String, String>();
        }

        ksDef.strategy_options.put("replication_factor", "1");
        client.system_add_keyspace(ksDef);
        client.set_keyspace("bigdata");

        String cql_Query = "create columnfamily test1 (name text, age text, address text, PRIMARY KEY(name,age))";

        client.execute_cql_query(ByteBuffer.wrap(("USE bigdata").getBytes("UTF-8")), Compression.NONE);

        client.execute_cql_query(ByteBuffer.wrap((cql_Query).getBytes("UTF-8")), Compression.NONE);

        /*
         * ColumnParent parent = new ColumnParent("test1");
         * 
         * List<AbstractType<?>> keyTypes = new ArrayList<AbstractType<?>>();
         * keyTypes.add(UTF8Type.instance); keyTypes.add(UTF8Type.instance);
         * CompositeType compositeKey = CompositeType.getInstance(keyTypes);
         * 
         * Builder builder = new Builder(compositeKey);
         * builder.add(ByteBuffer.wrap("1".getBytes()));
         * builder.add(ByteBuffer.wrap("2".getBytes())); ByteBuffer rowid =
         * builder.build();
         * 
         * Column column = new Column(); column.setName("value".getBytes());
         * column.setValue("aaa".getBytes());
         * column.setTimestamp(System.currentTimeMillis());
         * 
         * client.insert(rowid, parent, column, ConsistencyLevel.ONE);
         */

        ColumnParent parent = new ColumnParent("test1");

        List<AbstractType<?>> keyTypes = new ArrayList<AbstractType<?>>();
        keyTypes.add(UTF8Type.instance);
        keyTypes.add(UTF8Type.instance);
        CompositeType compositeKey = CompositeType.getInstance(keyTypes);

        Builder builder = new Builder(compositeKey);
        builder.add(ByteBuffer.wrap("3".getBytes()));
        builder.add(ByteBuffer.wrap("address".getBytes()));
        ByteBuffer columnName = builder.build();

        Column column = new Column();
        column.setName(columnName);
        column.setValue("4".getBytes());
        column.setTimestamp(System.currentTimeMillis());

        client.insert(ByteBuffer.wrap("1".getBytes()), parent, column, ConsistencyLevel.ONE);

        SlicePredicate slicePredicate = new SlicePredicate();
        slicePredicate.setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 10000));
        List<ColumnOrSuperColumn> columns = client.get_slice(ByteBuffer.wrap("1".getBytes()), parent, slicePredicate,
                ConsistencyLevel.ONE);

        for (ColumnOrSuperColumn col : columns)
        {
            if (col.column != null)
            {
                System.out.println(new String(col.getColumn().getName())); // Printing
                                                                           // "EMPTY"
                                                                           // String!
                System.out.println(new String(col.getColumn().getValue()));
            }
            else if (col.super_column != null)
            {

            }
            else if (col.counter_column != null)
            {

            }
            else if (col.counter_super_column != null)
            {

            }
        }

        // On cql select query.

        String selectQuery = "Select * from test1";
        CqlResult result = client.execute_cql_query(ByteBuffer.wrap(selectQuery.getBytes("UTF-8")), Compression.NONE);
        Iterator<CqlRow> rows = result.getRowsIterator();
        while (rows.hasNext())
        {
            CqlRow row = rows.next();
            List<Column> cols = row.getColumns();
            for (Column c : cols)
            {
                System.out.println(new String(c.getName()) + "=>" + new String(c.getValue()));

            }
        }

        ColumnPath path = new ColumnPath("test1");
        client.remove(ByteBuffer.wrap("1".getBytes()), path, System.currentTimeMillis(), ConsistencyLevel.ONE);

        // Insert after delete is not working for compound primary key?

        client.insert(ByteBuffer.wrap("1".getBytes()), parent, column, ConsistencyLevel.ONE);

        client.insert(ByteBuffer.wrap("2".getBytes()), parent, column, ConsistencyLevel.ONE);

        // while(result.)
    }

}
