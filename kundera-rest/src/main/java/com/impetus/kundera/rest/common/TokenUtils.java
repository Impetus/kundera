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

import java.util.UUID;

/**
 * Utility class for generating tokens
 * @author amresh.singh
 */
public class TokenUtils
{
    /**
     * Generates Application Token
     * @return
     */
    public static String generateApplicationToken() {
        return Constants.APPLICATION_TOKEN_PREFIX + "_" + UUID.randomUUID();
    }
    
    /**
     * Generates Session Token
     * @return
     */
    public static String generateSessionToken() {
        return Constants.SESSION_TOKEN_PREFIX + "_" + UUID.randomUUID();
    }  

}
