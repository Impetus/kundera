/**
 * 
 */
package com.impetus.client.mongodb;


/**
 * @author Kuldeep Mishra
 * 
 */
public interface MongoDBConstants
{
    public final static String CONNECTIONS = "mongodb.servers";

    public final static String SOCKET_TIMEOUT = "socket.timeout";

    public final static String READ_PREFERENCE = "read.preference";

    public final static String AUTO_CONNECT_RETRY = "autoconnect.retry";

    public static final String CONNECTION_PER_HOST = "connection.perhost";

    public static final String CONNECT_TIME_OUT = "connection.timeout";

    public static final String DB_DECODER_FACTORY = "dbdecoder.factory";

    public static final String DB_ENCODER_FACTORY = "dbencoder.factory";

    public static final String FSYNC = "fsync";

    public static final String MAX_AUTO_CONNECT_RETRY = "max.autoconnect.retry";

    public static final String J = "j";

    public static final String MAX_WAIT_TIME = "maxwait.time";

    public static final String SAFE = "safe";

    public static final String SOCKET_FACTORY = "socket.factory";

    public static final String TABCM = "threadsallowed.block.connectionmultiplier";

    public static final String W = "w";

    public static final String W_TIME_OUT = "w.timeout";

    public static final int DEFAULT_MAX_WAIT_TIME = 120000;

    public static final int DEFAULT_TABCM = 5;
}
