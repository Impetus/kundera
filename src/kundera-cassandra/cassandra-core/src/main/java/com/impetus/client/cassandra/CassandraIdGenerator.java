/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.cassandra;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.generator.TableGenerator;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;

/**
 * The Class CassandraIdGenerator.
 * 
 * @author: karthikp.manchala
 */
public class CassandraIdGenerator implements AutoGenerator, TableGenerator
{

    /** The Constant SYSTEM. */
    private static final String SYSTEM = "system";

    /** The Constant UUID. */
    private static final String UUID = "uuid";

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(CassandraIdGenerator.class);

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.generator.AutoGenerator#generate(com.impetus.kundera
     * .client.Client, java.lang.Object)
     */
    @Override
    public Object generate(Client<?> client, String dataType)
    {
        CqlResult cqlResult = ((CassandraClientBase) client).execute("SELECT NOW() FROM system_schema.columns LIMIT 1",
                ((CassandraClientBase) client).getRawClient(SYSTEM));

        CqlRow cqlRow = cqlResult.getRowsIterator().next();
        TimeUUIDType t = TimeUUIDType.instance;
        UUID timeUUID = t.compose(ByteBuffer.wrap(cqlRow.getColumns().get(0).getValue()));

        switch (dataType.toLowerCase())
        {
        case UUID:
            return timeUUID;

        default:
            return java.util.UUID.randomUUID();

        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.generator.TableGenerator#generate(com.impetus.kundera
     * .metadata.model.TableGeneratorDiscriptor,
     * com.impetus.kundera.client.ClientBase, java.lang.Object)
     */
    @Override
    public Object generate(TableGeneratorDiscriptor discriptor, ClientBase client, String dataType)
    {
        Cassandra.Client conn = ((CassandraClientBase) client).getRawClient(discriptor.getSchema());
        long latestCount = 0l;
        try
        {
            conn.set_keyspace(discriptor.getSchema());

            if (((CassandraClientBase) client).isCql3Enabled())
            {
                CQLTranslator translator = new CQLTranslator();
                ((CassandraClientBase) client).execute(translator.buildUpdateQuery(discriptor).toString(), conn);

                CqlResult result = ((CassandraClientBase) client).execute(translator.buildSelectQuery(discriptor)
                        .toString(), conn);

                for (CqlRow row : result.getRows())
                {
                    latestCount = ByteBufferUtil.toLong(ByteBuffer.wrap(row.getColumns().get(0).getValue()));
                }
            }
            else
            {

                ColumnPath columnPath = new ColumnPath(discriptor.getTable());
                columnPath.setColumn(discriptor.getValueColumnName().getBytes());

                try
                {
                    latestCount = conn.get(ByteBuffer.wrap(discriptor.getPkColumnValue().getBytes()), columnPath,
                            ((CassandraClientBase) client).getConsistencyLevel()).counter_column.value;
                }
                catch (NotFoundException e)
                {
                    log.warn("Counter value not found for {}, resetting it to zero.", discriptor.getPkColumnName());
                    latestCount = 0;
                }
                ColumnParent columnParent = new ColumnParent(discriptor.getTable());

                CounterColumn counterColumn = new CounterColumn(ByteBuffer.wrap(discriptor.getValueColumnName()
                        .getBytes()), 1);

                conn.add(ByteBuffer.wrap(discriptor.getPkColumnValue().getBytes()), columnParent, counterColumn,
                        ((CassandraClientBase) client).getConsistencyLevel());
            }
            if (latestCount == 0)
            {
                return (long) discriptor.getInitialValue();
            }
            else
            {
                return (latestCount + 1) * discriptor.getAllocationSize();
            }
        }
        catch (UnavailableException e)
        {
            log.error("Error while reading counter value from table{}, Caused by: .", discriptor.getTable(), e);
            throw new KunderaException(e);
        }
        catch (TimedOutException e)
        {
            log.error("Error while reading counter value from table{}, Caused by: .", discriptor.getTable(), e);
            throw new KunderaException(e);
        }
        catch (Exception e)
        {
            log.error("Error while using keyspace. Caused by: .", e);
            throw new KunderaException(e);
        }
    }

}
