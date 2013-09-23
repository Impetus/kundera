/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.impetus.kundera.ycsb.benchmark;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * @author Kuldeep mishra
 *
 */
public class RedisClient extends DB
{
    private Jedis jedis;

    public static final String HOST_PROPERTY = "hosts";

    public static final String PORT_PROPERTY = "port";

    public static final String PASSWORD_PROPERTY = "password";

    public static final String INDEX_KEY = "_indices";

    public void init() throws DBException
    {
        Properties props = getProperties();
        int port;

        String portString = props.getProperty(PORT_PROPERTY);
        if (portString != null)
        {
            port = Integer.parseInt(portString);
        }
        else
        {
            port = Protocol.DEFAULT_PORT;
        }
        String host = props.getProperty(HOST_PROPERTY);

        jedis = new Jedis(host, port);
        jedis.connect();

        String password = props.getProperty(PASSWORD_PROPERTY);
        if (password != null)
        {
            jedis.auth(password);
        }
    }

    public void cleanup() throws DBException
    {
        jedis.disconnect();
    }

    /*
     * Calculate a hash for a key to store it in an index. The actual return
     * value of this function is not interesting -- it primarily needs to be
     * fast and scattered along the whole space of doubles. In a real world
     * scenario one would probably use the ASCII values of the keys.
     */
    private double hash(String key)
    {
        return key.hashCode();
    }

    // XXX jedis.select(int index) to switch to `table`

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
    {
        if (fields == null)
        {
            StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
        }
        else
        {
            String[] fieldArray = (String[]) fields.toArray(new String[fields.size()]);
            List<String> values = jedis.hmget(key, fieldArray);

            Iterator<String> fieldIterator = fields.iterator();
            Iterator<String> valueIterator = values.iterator();

            while (fieldIterator.hasNext() && valueIterator.hasNext())
            {
                result.put(fieldIterator.next(), new StringByteIterator(valueIterator.next()));
            }
            assert !fieldIterator.hasNext() && !valueIterator.hasNext();
        }
        return result.isEmpty() ? 1 : 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {
        if (jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK"))
        {
         //  jedis.zadd(INDEX_KEY, hash(key), key);
            return 0;
        }
        return 1;
    }

    @Override
    public int delete(String table, String key)
    {
        return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? 1 : 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values)
    {
        return jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK") ? 0 : 1;
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result)
    {
        Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey), Double.POSITIVE_INFINITY, 0, recordcount);

        HashMap<String, ByteIterator> values;
        for (String key : keys)
        {
            values = new HashMap<String, ByteIterator>();
            read(table, key, fields, values);
            result.add(values);
        }

        return 0;
    }

    public static void main(String[] args)
    {

        RedisClient cli = new RedisClient();

        Properties props = new Properties();

        props.setProperty("hosts", "localhost");
        props.setProperty("password","Kundera@123");
        cli.setProperties(props);

        try
        {
            cli.init();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(0);
        }

        HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
        vals.put("age", new StringByteIterator("57"));
        vals.put("middlename", new StringByteIterator("bradley"));
        vals.put("favoritecolor", new StringByteIterator("blue"));
        int res = cli.insert("usertable", "BrianFrankCooper", vals);
        System.out.println("Result of insert: " + res);

        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        HashSet<String> fields = new HashSet<String>();
        fields.add("middlename");
        fields.add("age");
        fields.add("favoritecolor");
        res = cli.read("usertable", "BrianFrankCooper", null, result);
        System.out.println("Result of read: " + res);
        for (String s : result.keySet())
        {
            System.out.println("[" + s + "]=[" + result.get(s) + "]");
        }

        res = cli.delete("usertable", "BrianFrankCooper");
        System.out.println("Result of delete: " + res);

    }

}
