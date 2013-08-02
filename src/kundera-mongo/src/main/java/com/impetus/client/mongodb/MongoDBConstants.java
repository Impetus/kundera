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
package com.impetus.client.mongodb;

/**
 * Holds constants for kundera-mongo module
 * 
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

    // public static final int DEFAULT_MAX_WAIT_TIME = 120000;
    //
    // public static final int DEFAULT_TABCM = 5;
    public static final String CAPPED = "capped";

    public static final String SIZE = "size";

    public static final String MAX = "max";

    public static final String MIN = "min";

    public final static String WRITE_CONCERN = "write.concern";
}
