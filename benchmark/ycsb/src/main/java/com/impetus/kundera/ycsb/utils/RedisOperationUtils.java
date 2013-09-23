/**
 * 
 */
package com.impetus.kundera.ycsb.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import redis.clients.jedis.Jedis;

import common.Logger;

/**
 * @author Kuldeep Mishra
 * 
 */
public class RedisOperationUtils
{
    private static Logger logger = Logger.getLogger(RedisOperationUtils.class);

    /**
     * Stop mongo server.
     * 
     * @param port
     *            the runtime
     * @param br
     *            the br
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public void cleanRedisDatabase(String host, int port, String password) throws IOException, InterruptedException
    {
        logger.info("flushing db ..........");
        Jedis jedis = new Jedis(host, port);
        jedis.connect();
        if (password != null)
        {
            jedis.auth(password);
        }
        jedis.flushDB();
        jedis.disconnect();
    }

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
    public void startRedisServer(Runtime runtime, String startRedisServerCommand) throws IOException,
            InterruptedException
    {
        logger.info("Starting redis server at " + startRedisServerCommand + "...........");
        runtime.exec(startRedisServerCommand);
        Thread.sleep(35000);
        logger.info("started..............");
    }

    /**
     * Stop Redis server.
     * 
     * @param runtime
     *            the runtime
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public void stopRedisServer(Runtime runtime) throws IOException, InterruptedException
    {
        logger.info("Stoping redis server..");
        String line;
        Process ps = runtime.exec("ps -ux");
        InputStream is = ps.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        while ((line = br.readLine()) != null)
        {
            StringTokenizer tokenizer = new StringTokenizer(line, " ");
            if (line.contains("redis-server"))
            {
                System.out.println(line);
                tokenizer.nextElement();
                String nextElement = (String) tokenizer.nextElement();
                System.out.println(nextElement);
                runtime.exec("kill -9 " + nextElement);
                logger.info("stopped..............");
                break;
            }
        }
    }

    public static void main(String[] args)
    {
        RedisOperationUtils utils = new RedisOperationUtils();
        Runtime runtime = Runtime.getRuntime();
        String redisServerLocation = "/home/impadmin/software/redis-2.6.6/src/redis-server /home/impadmin/software/redis-2.6.6/redis.conf";
        try
        {
            utils.startRedisServer(runtime, redisServerLocation);
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
            utils.stopRedisServer(runtime);
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
