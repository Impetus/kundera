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

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.report.CSVSummaryReportModule;
import org.databene.contiperf.report.HtmlReportModule;
import org.databene.contiperf.report.ReportModule;
import org.junit.Rule;
import org.junit.Test;

import com.impetus.client.twitter.dao.Twitter;

/**
 * Test case for MongoDB.
 * 
 * @author amresh.singh
 */
public class TwingoTest extends TwitterTestBaseMongo
{

    /** The user id1. */
    String userId1;

    /** The user id2. */
    String userId2;

    /** The twitter. */
    Twitter twitter;

    @Rule
    public ContiPerfRule i = new ContiPerfRule(new ReportModule[] { new CSVSummaryReportModule(),
            new HtmlReportModule() });

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception
    {
        setUpInternal("mongoTest");
    }

    /**
     * Test on execute.
     */

    @Test
    @PerfTest(invocations = 10)
    public void testOnExecute()
    {
        executeTestSuite();
    }

    @Override
    protected void tearDown() throws Exception
    {
        tearDownInternal();
    }

    @Override
    void createSchema()
    {
        // No need to create schema, it will be created automatically
    }

    @Override
    void startServer()
    {
        // Currently no embedded server
    }

    @Override
    void stopServer()
    {
        // Currently no embedded server
    }

    @Override
    void deleteSchema()
    {
    }
}
