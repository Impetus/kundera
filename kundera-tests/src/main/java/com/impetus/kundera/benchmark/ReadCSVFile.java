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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read CSV File class reads the csv file.
 * 
 * @author Kuldeep Mishra
 * 
 */
public class ReadCSVFile
{
    public static String[] cvsSheetColumnName = new String[] { "serviceId", "duration", "invocations", "average", "" };

    public static Map<String, Integer> columnFieldIndexMap = new HashMap<String, Integer>();

    private static final Logger log = LoggerFactory.getLogger(ReadCSVFile.class);

    /**
     * Return csv file content as a map.
     * 
     * @param csvFile
     * @return
     */
    public static Map<String, TestResult> getCSVFileInfo(String csvFile)
    {
        Map<String, TestResult> testCaseDataMap = new HashMap<String, TestResult>();
        Map<String, String> summatyFieldToValueMap = new HashMap<String, String>();
        String serviceId = null;
        String duration = null;
        String invocations = null;
        String average = null;

        if (csvFile != null)
        {
            FileReader fileReader = null;
            try
            {
                fileReader = new FileReader(csvFile);
            }
            catch (Exception e)
            {
                log.error("File not found : " + e);
            }
            try
            {
                if (fileReader != null)
                {

                    BufferedReader br = new BufferedReader(fileReader);
                    log.info("Reading file : " + csvFile);
                    String line = "";
                    StringTokenizer st = null;

                    int lineNumber = 0;
                    int tokenNumber = 0;

                    // read comma separated file line by line
                    while ((line = br.readLine()) != null)
                    {

                        lineNumber++;
                        int i = 0;
                        // use comma as token separator
                        st = new StringTokenizer(line, ",");

                        while (st.hasMoreTokens())
                        {
                            tokenNumber++;
                            // display csv values
                            String token = st.nextToken();

                            if (lineNumber == 1)
                            {
                                if (token.equals(cvsSheetColumnName[i]))
                                {
                                    columnFieldIndexMap.put(cvsSheetColumnName[i], tokenNumber);
                                    i++;
                                }
                            }
                            else
                            {
                                if (i < 4 && tokenNumber == columnFieldIndexMap.get(cvsSheetColumnName[i]))
                                {
                                    summatyFieldToValueMap.put(cvsSheetColumnName[i], token);
                                    i++;
                                }
                            }
                        }
                        serviceId = summatyFieldToValueMap.get(cvsSheetColumnName[0]);
                        duration = summatyFieldToValueMap.get(cvsSheetColumnName[1]);
                        invocations = summatyFieldToValueMap.get(cvsSheetColumnName[2]);
                        average = summatyFieldToValueMap.get(cvsSheetColumnName[3]);
                        if (serviceId != null)
                        {
                            testCaseDataMap.put(serviceId,
                                    new TestResult(duration != null ? Double.parseDouble(duration) : Double.NaN,
                                            invocations != null ? Integer.parseInt(invocations) : 0,
                                            average != null ? Double.parseDouble(average) : Double.NaN));
                        }
                        // reset token number
                        tokenNumber = 0;
                    }
                }
            }
            catch (IOException e)
            {
                log.error("CSV file cannot be read : " + e);
            }
            return testCaseDataMap;
        }
        return testCaseDataMap;
    }
}
