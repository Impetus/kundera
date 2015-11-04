/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.oraclenosql.schemamanager;

import java.util.HashMap;

/**
 * The Class OracleNoSQLValidationClassMapper.
 */
public class OracleNoSQLValidationClassMapper
{

    /** The Constant validationClassMapper. */
    private final static HashMap<String, String> validationClassMapper = new HashMap<String, String>();

    static
    {
        validationClassMapper.put("int", "integer");
        validationClassMapper.put("integer", "integer");
        validationClassMapper.put("byte", "binary");
        validationClassMapper.put("short", "binary");
        validationClassMapper.put("bigdecimal", "binary");
        validationClassMapper.put("biginteger", "binary");
        validationClassMapper.put("date", "binary");
        validationClassMapper.put("calendar", "binary");
        validationClassMapper.put("char", "string");
        validationClassMapper.put("character", "string");
        validationClassMapper.put("long", "long");
        validationClassMapper.put("string", "string");
        validationClassMapper.put("boolean", "boolean");
        validationClassMapper.put("float", "float");
        validationClassMapper.put("double", "double");
        validationClassMapper.put("file", "binary");
        validationClassMapper.put("byte[]", "binary");
    }

    /**
     * Gets the valid type.
     * 
     * @param type
     *            the type
     * @return the valid type
     */
    public static String getValidType(String type)
    {
        return (validationClassMapper.get(type) == null) ? "binary" : validationClassMapper.get(type);
    }
}
