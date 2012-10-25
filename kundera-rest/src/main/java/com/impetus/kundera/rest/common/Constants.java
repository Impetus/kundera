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
 * Holds constants
 * 
 * @author amresh.singh
 */
public interface Constants
{
    public static final String KUNDERA_API_PATH = "/kundera/api";

    public static final String KUNDERA_REST_RESOURCES_PACKAGE = "com.impetus.kundera.rest.resources";

    /** Token prefixes */
    public static final String APPLICATION_TOKEN_PREFIX = "AT";

    public static final String SESSION_TOKEN_PREFIX = "ST";

    /** Header Names */
    public static final String APPLICATION_TOKEN_HEADER_NAME = "x-at";

    public static final String SESSION_TOKEN_HEADER_NAME = "x-st";
    
    public static final String QUERY_PARAMS_HEADER_NAME_PREFIX = "x-param-";   
    

    /** Resources */
    public static final String APPLICATION_RESOURCE_PATH = "/application";

    public static final String SESSION_RESOURCE_PATH = "/session";

    public static final String TRANSACTION_RESOURCE_PATH = "/tx";

    public static final String CRUD_RESOURCE_PATH = "/crud";

    public static final String JPA_QUERY_RESOURCE_PATH = "/query";
    
    public static final String NATIVE_QUERY_RESOURCE_PATH = "/nativeQuery";

    public static final String META_DATA_RESOURCE_PATH = "/metadata";
    
    public static final String NAMED_QUERY_ALL = "all";

}
