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
import java.util.StringTokenizer;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.impetus.kundera.tests.cli.CleanupUtilities;
import com.impetus.kundera.tests.crossdatastore.pickr.dao.Pickr;
import com.impetus.kundera.tests.crossdatastore.pickr.dao.PickrImpl;

/**
 * @author amresh.singh
 * 
 */
public abstract class PickrBaseTest
{
    protected static final boolean RUN_IN_EMBEDDED_MODE = true;

    protected static final boolean AUTO_MANAGE_SCHEMA = true;

    protected Pickr pickr;

    protected int photographerId;

    protected String pu = "piccandra,picmysql,picongo";

    protected void setUp() throws Exception
    {
        if (RUN_IN_EMBEDDED_MODE)
        {
            startServer();
        }
        photographerId = 1;
        pickr = new PickrImpl(pu);
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
        pickr.close();
        if (RUN_IN_EMBEDDED_MODE)
        {
//            stopServer();
        }
        stopServer();
        StringTokenizer tokenizer = new StringTokenizer(pu, ",");
        while (tokenizer.hasMoreTokens())
        {
            CleanupUtilities.cleanLuceneDirectory(tokenizer.nextToken());
        }
    }

    protected abstract void addPhotographer();

    protected abstract void updatePhotographer();

    protected abstract void getPhotographer();

    protected abstract void getAllPhotographers();

    protected abstract void deletePhotographer();

    protected abstract void startServer() throws IOException, TException, InvalidRequestException,
            UnavailableException, TimedOutException, SchemaDisagreementException;

    protected abstract void stopServer();

}
