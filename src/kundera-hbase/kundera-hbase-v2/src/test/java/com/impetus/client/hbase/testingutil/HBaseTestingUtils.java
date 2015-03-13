/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.testingutil;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.impetus.kundera.KunderaException;

/**
 * The Class HBaseTestingUtils.
 * 
 * @author Pragalbh Garg
 */
public final class HBaseTestingUtils
{

    /**
     * Drop schema.
     * 
     * @param databaseName
     *            the database name
     */
    public static void dropSchema(String databaseName)
    {
        try
        {
            Connection connection = ConnectionFactory.createConnection();
            try
            {
                HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
                admin.disableTables(databaseName + ":.*");
                admin.deleteTables(databaseName + ":.*");
                admin.deleteNamespace(databaseName);
            }
            catch (Exception e)
            {
                // do nothing
            }
            finally
            {
                connection.close();
            }
        }
        catch (IOException e)
        {
            throw new KunderaException("Could not connect to database, caused by:", e);
        }

    }

}
