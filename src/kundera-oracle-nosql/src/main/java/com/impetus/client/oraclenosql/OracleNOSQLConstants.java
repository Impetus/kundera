/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql;

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;

/**
 * Constants used in OracleNoSQL implementation of Kundera
 * 
 * @author amresh.singh
 */
public interface OracleNOSQLConstants
{
    /** LOB Constants */
    public static final int DEFAULT_WRITE_TIMEOUT_SECONDS = 5;

    public static final Durability DEFAULT_DURABILITY = Durability.COMMIT_WRITE_NO_SYNC;

    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

    public static final Consistency DEFAULT_CONSISTENCY = Consistency.NONE_REQUIRED;

    public static final String LOB_SUFFIX = ".lob";

    public static final int OUTPUT_BUFFER_SIZE = 1024;

    public static final String SECONDARY_INDEX_SUFFIX = "_idx";

}
