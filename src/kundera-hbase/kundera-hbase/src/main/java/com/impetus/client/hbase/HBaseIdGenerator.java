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
package com.impetus.client.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.admin.HBaseDataHandler;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.generator.TableGenerator;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;

/**
 * The Class HBaseIdGenerator.
 * 
 * @author: karthikp.manchala
 */
public class HBaseIdGenerator implements TableGenerator
{
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(HBaseIdGenerator.class);

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.generator.TableGenerator#generate(com.impetus.kundera
     * .metadata.model.TableGeneratorDiscriptor,
     * com.impetus.kundera.client.ClientBase, java.lang.Object)
     */
    @Override
    public Object generate(final TableGeneratorDiscriptor discriptor,
            ClientBase client, String dataType)
    {
        try
        {
            Long latestCount = ((HBaseClient) client).getRequestExecutor().execute(
                    new TableRequest<Long>(discriptor.getSchema()) {
                        protected Long execute(Table table) throws IOException {
                            return table.incrementColumnValue(discriptor.getPkColumnValue().getBytes(),
                                    discriptor.getTable().getBytes(),
                                    discriptor.getValueColumnName().getBytes(), 1);
                        }
                    });
            if (latestCount == 1)
            {
                return (long) discriptor.getInitialValue();
            }
            else if (discriptor.getAllocationSize() == 1)
            {
                return latestCount + discriptor.getInitialValue();
            }
            else
            {
                return (latestCount - 1) * discriptor.getAllocationSize() + discriptor.getInitialValue();
            }
        }
        catch (IOException ioex)
        {
            log.error("Error while generating id for entity, Caused by: .", ioex);
            throw new KunderaException(ioex);
        }
    }
}
