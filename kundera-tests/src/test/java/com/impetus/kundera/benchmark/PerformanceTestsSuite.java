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
package com.impetus.kundera.benchmark;

import java.io.IOException;
import java.util.Map;

import jxl.write.WriteException;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.junit.ContiPerfSuiteRunner;
import org.databene.contiperf.report.CSVSummaryReportModule;
import org.databene.contiperf.report.HtmlReportModule;
import org.databene.contiperf.report.ReportModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;

import com.impetus.client.RedisClientTest;
import com.impetus.client.gis.MongoGISTest;

/**
 * A test suite for performance benchmarking , add test classes in
 * {@code @SuiteClasses} to benchmark.
 * 
 * @author Kuldeep Mishra
 * 
 */
@SuiteClasses({ MongoGISTest.class, RedisClientTest.class })
@PerfTest(threads = 1, invocations = 2)
@RunWith(ContiPerfSuiteRunner.class)
public class PerformanceTestsSuite
{
    private static String[] excelSheetColumnName = new String[] { "serviceId", "duration", "invocations", "average",
            "averagedelta" };

    private static String csvFile = "./target/contiperf-report/summary.csv";

    private static String xlsFile = null;

    private static Map<String, TestResult> testCaseDataMapBefore;

    private static Map<String, TestResult> testCaseDataMapAfter;

    @Rule
    public ContiPerfRule i = new ContiPerfRule(new ReportModule[] { new CSVSummaryReportModule(),
            new HtmlReportModule() });

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        testCaseDataMapBefore = ReadCSVFile.getCSVFileInfo(csvFile);
        xlsFile = System.getProperty("fileName");
    }

    @AfterClass
    public static void afterClass() throws IOException, WriteException
    {
        testCaseDataMapAfter = ReadCSVFile.getCSVFileInfo(csvFile);
        generateDelta(testCaseDataMapBefore, testCaseDataMapAfter);
        if (xlsFile != null)
        {
            WriteToExcelFile.writeInXlsFile(testCaseDataMapAfter, xlsFile, excelSheetColumnName);
        }
    }

    /**
     * @param testCaseDataMapBefore2
     * @param testCaseDataMapAfter2
     */
    private static void generateDelta(Map<String, TestResult> testCaseDataMapBefore,
            Map<String, TestResult> testCaseDataMapAfter)
    {
        if (testCaseDataMapBefore != null && !testCaseDataMapBefore.isEmpty() && testCaseDataMapAfter != null
                && !testCaseDataMapAfter.isEmpty())
        {
            for (String s : testCaseDataMapBefore.keySet())
            {
                TestResult afterTestResult = testCaseDataMapAfter.get(s);
                if (afterTestResult != null)
                {
                    Double avgDelta = (((double) (testCaseDataMapBefore.get(s).getAvgTime() - afterTestResult
                            .getAvgTime())) / (testCaseDataMapBefore.get(s).getAvgTime())) / 100;
                    afterTestResult.setDelta(avgDelta);
                }
            }
        }
    }
}
