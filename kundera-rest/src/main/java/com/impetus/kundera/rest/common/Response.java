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
package com.impetus.kundera.rest.common;

/**
 * Constants for REST responses
 * 
 * @author amresh.singh
 */
public interface Response
{
    public static final String DELETE_AT_SUCCESS = "APPLICATION_TOKEN_ DELETE_SUCCESS";

    public static final String DELETE_AT_FAILED = "APPLICATION_TOKEN_ DELETE_FAILED";

    public static final String DELETE_ST_SUCCESS = "SESSION_TOKEN_ DELETE_SUCCESS";

    public static final String DELETE_ST_FAILED = "SESSION_TOKEN_ DELETE_FAILED";

    public static final String GET_ST_FAILED = "SESSION_TOKEN_ GET_FAILED";

    public static final String PUT_ST_FAILED = "SESSION_TOKEN_ PUT_FAILED";

    public static final String PUT_ST_SUCCESS = "SESSION_TOKEN_ PUT_SUCCESS";

    public static final String GET_TX_SUCCESS = "GET_TX_SUCCESS";

    public static final String GET_TX_FAILED = "GET_TX_FAILED";

    public static final String POST_TX_SUCCESS = "POST_TX_SUCCESS";

    public static final String POST_TX_FAILED = "POST_TX_FAILED";

    public static final String DELETE_TX_SUCCESS = "DELETE_TX_SUCCESS";

    public static final String DELETE_TX_FAILED = "DELETE_TX_FAILED";
}
