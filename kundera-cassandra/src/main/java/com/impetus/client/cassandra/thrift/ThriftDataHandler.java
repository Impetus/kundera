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
package com.impetus.client.cassandra.thrift;

import org.apache.cassandra.thrift.SuperColumn;

import com.impetus.client.cassandra.datahandler.CassandraDataHandler;
import com.impetus.client.cassandra.datahandler.CassandraDataHandlerBase;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Data handler for Thrift Clients 
 * @author amresh.singh
 */
public final class ThriftDataHandler extends CassandraDataHandlerBase implements CassandraDataHandler
{
    
    @Override
    public <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, DataRow<SuperColumn> tr) throws Exception {
        return super.fromThriftRow(clazz, m, tr);
    }

}
