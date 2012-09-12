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
package com.impetus.kundera.configure;

import java.io.File;

import com.thoughtworks.xstream.XStream;

/**
 * 
 * @author Kuldeep Mishra
 * 
 */
public class AbstractPropertyReader
{
    public ClientProperties parseXML(String propertyName)
    {
        XStream stream = new XStream();
        stream.alias("clientProperties", ClientProperties.class);
        stream.alias("dataStore", ClientProperties.DataStore.class);
        stream.alias("schema", ClientProperties.DataStore.Schema.class);
        stream.alias("table", ClientProperties.DataStore.Schema.Table.class);
        stream.alias("dataCenter", ClientProperties.DataStore.Schema.DataCenter.class);
        stream.alias("connection", ClientProperties.DataStore.Connection.class);
        stream.alias("server", ClientProperties.DataStore.Connection.Server.class);

        Object o = stream.fromXML(new File(propertyName));
        return (ClientProperties) o;
        // configurationProperties.put(pu, (KunderaClientProperties) o);
    }
    
    protected PropertyType getProperty(String propertyName)
    {
        return PropertyType.value(propertyName);
    }

    public enum PropertyType
    {
        xml, properties;

        private static final String DELIMETER = ".";

        static PropertyType value(String propertyName)
        {
            if (isXml(propertyName))
            {
                return xml;
            }
            else if (isProperties(propertyName))
            {
                return properties;
            }
            throw new IllegalArgumentException("unsupported property provided format:" + propertyName);
        }
        
        public static boolean isXml(String propertyName)
        {
            return propertyName.endsWith(DELIMETER + xml);
        }

        public static boolean isProperties(String propertyName)
        {
            return propertyName.endsWith(DELIMETER+properties);
        }
    }
}
