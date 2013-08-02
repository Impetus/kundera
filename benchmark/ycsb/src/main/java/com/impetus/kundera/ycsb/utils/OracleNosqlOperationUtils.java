/**
 * 
 */
package com.impetus.kundera.ycsb.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import common.Logger;

/**
 * @author Kuldeep Mishra
 * 
 */
public class OracleNosqlOperationUtils
{
    private static Logger logger = Logger.getLogger(OracleNosqlOperationUtils.class);

    /**
     * Start Redis server.
     * 
     * @param runtime
     *            the runtime
     * @return the process
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public void cleanAndStartOracleServer(Runtime runtime) throws IOException, InterruptedException
    {
        logger.info("Starting oracle server ...........");
        runtime.exec("src/main/resources/startOracleServer.sh");
        Thread.sleep(35000);
        logger.info("started..............");
    }

    /**
     * Stop Redis server.
     * 
     * @param runtime
     *            the runtime
     * @param performDeleteData
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public void stopOracleServer(Runtime runtime) throws IOException, InterruptedException
    {
        logger.info("Stoping oracle server..");
        Process process = runtime.exec("jps");
        InputStream is = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        String line = null;
        while ((line = br.readLine()) != null)
        {
            if (line.contains("kvstore.jar"))
            {
                int idx;
                idx = line.indexOf("kvstore.jar");
                runtime.exec("kill -9 " + line.substring(0, idx - 1));
                TimeUnit.SECONDS.sleep(5);
            }
        }

        logger.info("stopped..............");
    }

    public static void main(String[] args)
    {
        OracleNosqlOperationUtils utils = new OracleNosqlOperationUtils();
        Runtime runtime = Runtime.getRuntime();
        try
        {
            utils.cleanAndStartOracleServer(runtime);
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try
        {
            utils.stopOracleServer(runtime);
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
