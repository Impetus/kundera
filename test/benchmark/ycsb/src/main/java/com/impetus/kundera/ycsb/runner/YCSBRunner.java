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
package com.impetus.kundera.ycsb.runner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.impetus.kundera.ycsb.entities.PerformanceNoInfo;
import com.impetus.kundera.ycsb.utils.HibernateCRUDUtils;
import common.Logger;

/**
 * @author vivek.mishra
 * 
 */
public abstract class YCSBRunner
{

    private HibernateCRUDUtils crudUtils;

    protected String schema;

    protected String columnFamilyOrTable;

    private String clientjarlocation;

    private String ycsbJarLocation;

    private String propertyFile;

    protected int noOfThreads;

    protected String runType;

    protected double releaseNo;

    protected String host;

    protected int port;

    protected String[] clients;

    protected String workLoad;

    protected String currentClient;

    protected String password;

    protected boolean isUpdate;

    protected Map<String, BigDecimal> timeTakenByClient = new HashMap<String, BigDecimal>();

    private static Logger logger = Logger.getLogger(YCSBRunner.class);

    public YCSBRunner(final String propertyFile, final Configuration config)
    {
        this.propertyFile = propertyFile;
        ycsbJarLocation = config.getString("ycsbjar.location");
        clientjarlocation = config.getString("clientjar.location");
        host = config.getString("hosts");
        schema = config.getString("schema");
        columnFamilyOrTable = config.getString("columnfamilyOrTable");
        releaseNo = config.getDouble("release.no");
        runType = config.getString("run.type", "load");
        port = config.getInt("port");
        password = config.getString("password");
        clients = config.getStringArray("clients");
        isUpdate = config.containsKey("update");
        crudUtils = new HibernateCRUDUtils();
    }

    public void run(final String workLoad, final int threadCount) throws IOException
    {
        int runCounter = crudUtils.getMaxRunSequence(new Date(), runType);
        runCounter = runCounter + 1;
        noOfThreads = threadCount;
        // id column of performanceNoInfo table
        Date id = new Date();

        int counter = 1;
        for (String client : clients)
        {
            currentClient = client;
            if (clientjarlocation != null && ycsbJarLocation != null && client != null && runType != null
                    && host != null && schema != null && columnFamilyOrTable != null)
            {
                Runtime runtime = Runtime.getRuntime();
                counter++;
                String runCommand = getCommandString(client, workLoad);

                logger.info(runCommand);
                double totalTime = 0.0;
                long noOfOperations = 0;

                Process process = runtime.exec(runCommand);

                InputStream er = process.getErrorStream();
                InputStreamReader esr = new InputStreamReader(er);
                BufferedReader ebr = new BufferedReader(esr);
                String line;
                while ((line = ebr.readLine()) != null)
                {
                    logger.debug(line);
                }

                InputStream is = process.getInputStream();
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                BigDecimal avgLatency = null;
                BigDecimal throughput = null;

                boolean processed = false;
                while ((line = br.readLine()) != null)
                {
                    logger.debug(line);

                    processed = true;
                    if (line.contains("RunTime"))
                    {
                        totalTime = Double.parseDouble(line.substring(line.lastIndexOf(", ") + 2));
                        logger.info("Total time taken " + totalTime);
                    }
                    if (line.contains("Operations") && noOfOperations == 0)
                    {
                        noOfOperations = Long.parseLong(line.substring(line.lastIndexOf(", ") + 2));
                        logger.info("Total no of oprations " + noOfOperations);
                    }
                    if (line.contains("Throughput"))
                    {

                        throughput = new BigDecimal(line.substring(line.lastIndexOf(", ") + 2));
                        logger.info("Throughput(ops/sec) " + line);
                    }
                    if (line.contains("AverageLatency"))
                    {
                        if (avgLatency == null)
                        {
                            avgLatency = new BigDecimal(line.substring(line.lastIndexOf(", ") + 2));
                            logger.info("AverageLatency " + line);
                        }
                    }
                    /*
                     * if(line.contains("MinLatency")) {
                     * logger.info("MinLatency " + line); }
                     * if(line.contains("MaxLatency")) {
                     * logger.info("MaxLatency " + line); }
                     */
                    // if(!(line.contains("CLEANUP") || line.contains("UPDATE")
                    // || line.contains("INSERT") )){
                    // logger.info(line);
                    // }
                }

                if (!processed)
                {
                    is = process.getErrorStream();
                    isr = new InputStreamReader(is);
                    br = new BufferedReader(isr);
                    line = null;
                    while ((line = br.readLine()) != null)
                    {
                        logger.info(line);

                    }
                    throw new RuntimeException("Error while processing");
                }

                PerformanceNoInfo info = new PerformanceNoInfo(id, releaseNo,
                        client.substring(client.lastIndexOf(".") + 1), runType, noOfThreads, noOfOperations, totalTime,
                        runCounter);

                if (avgLatency != null)
                {
                    info.setAvgLatency(avgLatency.round(MathContext.DECIMAL32));
                }

                if (throughput != null)
                {
                    info.setThroughput(throughput.round(MathContext.DECIMAL32));
                }
                crudUtils.persistInfo(info);
                timeTakenByClient.put(client, throughput);
            }
        }

        sendMail();
    }

    protected String getCommandString(String clazz, String workLoad)
    {
        StringBuilder command = new StringBuilder("java -Xms512M -Xmx2048M -cp ");
        command.append(clientjarlocation);
        command.append(":");
        command.append(ycsbJarLocation);
        command.append(" com.yahoo.ycsb.Client -db ");
        command.append(clazz);
        command.append(" -s -P ");
        command.append(workLoad);
        command.append(" -P ");
        command.append(propertyFile);
        if (noOfThreads > 1)
        {
            command.append(" -threads ");
            command.append(noOfThreads);
        }
        command.append(" -");
        command.append(runType);

        return command.toString();
    }

    public abstract void startServer(boolean performDelete, Runtime runTime);

    public abstract void stopServer(Runtime runTime);

    protected abstract void sendMail();

    /**
     * If multiple clients are running, clear data for first time but only in
     * case of load.
     * 
     * @param counter
     *            client counter
     * @return true, if delete needs to be performed, else false.s
     */
    private boolean performDelete(int counter)
    {
        if (runType.equals("load"))
        {
            return counter == 1;
        }

        return false;
    }
}
