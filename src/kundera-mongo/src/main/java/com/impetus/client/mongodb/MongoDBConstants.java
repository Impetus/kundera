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
 * Holds constants for kundera-mongo module.
 * 
 * @author Kuldeep Mishra
 */
public interface MongoDBConstants
{

    /** The Constant CONNECTIONS. */
    public static final String CONNECTIONS = "mongodb.servers";

    /** The Constant SOCKET_TIMEOUT. */
    public static final String SOCKET_TIMEOUT = "socket.timeout";

    /** The Constant READ_PREFERENCE. */
    public static final String READ_PREFERENCE = "read.preference";

    /** The Constant AUTO_CONNECT_RETRY. */
    public static final String AUTO_CONNECT_RETRY = "autoconnect.retry";

    /** The Constant CONNECTION_PER_HOST. */
    public static final String CONNECTION_PER_HOST = "connection.perhost";

    /** The Constant CONNECT_TIME_OUT. */
    public static final String CONNECT_TIME_OUT = "connection.timeout";

    /** The Constant DB_DECODER_FACTORY. */
    public static final String DB_DECODER_FACTORY = "dbdecoder.factory";

    /** The Constant DB_ENCODER_FACTORY. */
    public static final String DB_ENCODER_FACTORY = "dbencoder.factory";

    /** The Constant FSYNC. */
    public static final String FSYNC = "fsync";

    /** The Constant MAX_AUTO_CONNECT_RETRY. */
    public static final String MAX_AUTO_CONNECT_RETRY = "max.autoconnect.retry";

    /** The Constant J. */
    public static final String J = "j";

    /** The Constant MAX_WAIT_TIME. */
    public static final String MAX_WAIT_TIME = "maxwait.time";

    /** The Constant SAFE. */
    public static final String SAFE = "safe";

    /** The Constant SOCKET_FACTORY. */
    public static final String SOCKET_FACTORY = "socket.factory";

    /** The Constant TABCM. */
    public static final String TABCM = "threadsallowed.block.connectionmultiplier";

    /** The Constant W. */
    public static final String W = "w";

    /** The Constant W_TIME_OUT. */
    public static final String W_TIME_OUT = "w.timeout";

    // public static final int DEFAULT_MAX_WAIT_TIME = 120000;

    // public static final int DEFAULT_TABCM = 5;

    /** The Constant CAPPED. */
    public static final String CAPPED = "capped";

    /** The Constant SIZE. */
    public static final String SIZE = "size";

    /** The Constant MAX. */
    public static final String MAX = "max";

    /** The Constant MIN. */
    public static final String MIN = "min";

    /** The Constant WRITE_CONCERN. */
    public final static String WRITE_CONCERN = "write.concern";

    /** The Constant ORDERED_BULK_OPERATION. */
    public final static String ORDERED_BULK_OPERATION = "ordered.bulk.operation";

    /** The Constant REPLICA_SET_NAME. */
    public final static String REPLICA_SET_NAME = "replica.set.name";
}
