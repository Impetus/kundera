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
package com.impetus.client.cassandra;

import java.nio.ByteBuffer;
import java.util.List;

import javax.persistence.PersistenceException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.impetus.client.cassandra.pelops.PelopsClient;
import com.impetus.client.cassandra.pelops.PelopsUtils;
import com.impetus.client.cassandra.thrift.ThriftRow;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;

/**
 * Base Class for all Cassandra Clients
 * Contains methods that are applicable to (bot not specific to) different Cassandra clients.
 * @author amresh.singh
 */
public abstract class CassandraClientBase extends ClientBase
{
    
    /** log for this class. */
    private static Log log = LogFactory.getLog(CassandraClientBase.class);
    
    /**
     * Populates foreign key as column.
     * 
     * @param rlName
     *            relation name
     * @param rlValue
     *            relation value
     * @param timestamp
     *            the timestamp
     * @return the column
     * @throws PropertyAccessException
     *             the property access exception
     */
    protected Column populateFkey(String rlName, String rlValue, long timestamp) throws PropertyAccessException
    {
        Column col = new Column();
        col.setName(PropertyAccessorFactory.STRING.toBytes(rlName));
        col.setValue(rlValue.getBytes());
        col.setTimestamp(timestamp);
        return col;
    }
    
    /**
     * Adds relation foreign key values as thrift column/ value to thrift row
     * 
     * @param metadata
     * @param tf
     * @param relations
     */
    protected void addRelationsToThriftRow(EntityMetadata metadata, ThriftRow tf,
            List<RelationHolder> relations)
    {
        long timestamp = System.currentTimeMillis();
        
        if (relations != null)
        {
            for (RelationHolder rh : relations)
            {
                String linkName = rh.getRelationName();
                String linkValue = rh.getRelationValue();

                if (linkName != null && linkValue != null)
                {
                    if (metadata.getEmbeddedColumnsAsList().isEmpty())
                    {
                        if (metadata.isCounterColumnType())
                        {
                            CounterColumn col = populateCounterFkey(linkName, linkValue);
                            tf.addCounterColumn(col);
                        }
                        else
                        {
                            Column col = populateFkey(linkName, linkValue, timestamp);
                            tf.addColumn(col);
                        }

                    }
                    else
                    {
                        if (metadata.isCounterColumnType())
                        {
                            CounterSuperColumn counterSuperColumn = new CounterSuperColumn();
                            counterSuperColumn.setName(linkName.getBytes());
                            CounterColumn column = populateCounterFkey(linkName, linkValue);
                            counterSuperColumn.addToColumns(column);
                            tf.addCounterSuperColumn(counterSuperColumn);
                        }
                        else
                        {
                            SuperColumn superColumn = new SuperColumn();
                            superColumn.setName(linkName.getBytes());
                            Column column = populateFkey(linkName, linkValue, timestamp);
                            superColumn.addToColumns(column);
                            tf.addSuperColumn(superColumn);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * @param rlName
     * @param rlValue
     * @return
     */
    private CounterColumn populateCounterFkey(String rlName, String rlValue)
    {
        CounterColumn counterCol = new CounterColumn();
        counterCol.setName(PropertyAccessorFactory.STRING.toBytes(rlName));
        counterCol.setValue(new Long(rlValue));
        return counterCol;
    }
    
    /**
     * Deletes record for given primary key from counter column family
     * @param pKey
     * @param metadata
     */
    protected void deleteRecordFromCounterColumnFamily(Object pKey, EntityMetadata metadata, ConsistencyLevel consistencyLevel, Cassandra.Client cassandra_client)
    {
        ColumnPath path = new ColumnPath(metadata.getTableName());
        
        try
        {
            cassandra_client.remove_counter(ByteBuffer.wrap(pKey.toString().getBytes()), path, consistencyLevel);              
            
        }
        catch (InvalidRequestException ire)
        {
            log.error("Error during executing delete, Caused by :" + ire.getMessage());
            throw new PersistenceException(ire);
        }
        catch (UnavailableException ue)
        {
            log.error("Error during executing delete, Caused by :" + ue.getMessage());
            throw new PersistenceException(ue);
        }
        catch (TimedOutException toe)
        {
            log.error("Error during executing delete, Caused by :" + toe.getMessage());
            throw new PersistenceException(toe);
        }
        catch (TException te)
        {
            log.error("Error during executing delete, Caused by :" + te.getMessage());
            throw new PersistenceException(te);
        }
    }  
    

}
