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
package com.impetus.client.es;

/**
 * @author Amit Kumar
 *
 */

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.impetus.kundera.query.AggregationBaseTest;

/**
 * The Class ESAggregationTest.
 */
public class ESAggregationTest extends AggregationBaseTest
{
    private static Node node = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        if (!checkIfServerRunning())
        {
            ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
            builder.put("path.data", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }
    }

    /**
     * Check if server running.
     * 
     * @return true, if successful
     */
    private static boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 9300);
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
    
    @Override
    public void setPersistenceUnitName()
    {
        _PU = "esAggregationPU";
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (node != null)
        {
            node.close();
        }
    }
}