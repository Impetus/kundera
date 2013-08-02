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
package com.impetus.kundera.rest.converters;

import java.util.Collection;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.StreamUtils;

/**
 * Converts a Collection object to XML/ JSON representation and vice-versa
 * 
 * @author amresh
 * 
 */
public class CollectionConverter
{
    private static Logger log = LoggerFactory.getLogger(CollectionConverter.class);

    /**
     * Converts a collection of <code>genericClass</code> objects to String
     * representation
     * 
     * @param input
     * @param genericClass
     * @param mediaType
     * @return
     */
    public static String toString(Collection<?> input, Class<?> genericClass, String mediaType)
    {
        if (MediaType.APPLICATION_XML.equals(mediaType))
        {
            StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>")
                    .append("<").append(genericClass.getSimpleName().toLowerCase()).append("s>");
            for (Object obj : input)
            {
                if (obj != null)
                {
                    String s = JAXBUtils.toString(genericClass, obj, mediaType);

                    if (s.startsWith("<?xml"))
                    {
                        s = s.substring(s.indexOf(">") + 1, s.length());
                    }
                    sb.append(s);
                }
            }
            sb.append("<").append(genericClass.getSimpleName().toLowerCase()).append("s>");
            return sb.toString();
        }
        else
        {
            return null;
        }
    }

    /**
     * Converts a String representation to collection of
     * <code>genericClass</code> objects
     * 
     * @param input
     * @param collectionClass
     * @param genericClass
     * @param mediaType
     * @return
     */
    public static Collection toCollection(String input, Class<?> collectionClass, Class<?> genericClass,
            String mediaType)
    {

        try
        {
            if (MediaType.APPLICATION_XML.equals(mediaType))
            {
                Collection c = (Collection) collectionClass.newInstance();
                if (input.startsWith("<?xml"))
                {
                    input = input.substring(input.indexOf(">") + 1, input.length());
                }

                input = input.replaceAll("<" + genericClass.getSimpleName().toLowerCase() + "s>", "");

                while (!input.equals(""))
                {
                    int i = input.indexOf("</" + genericClass.getSimpleName().toLowerCase() + ">");
                    String s = input.substring(0, i + 3 + genericClass.getSimpleName().length());
                    input = input.substring(i + 3 + genericClass.getSimpleName().length(), input.length());
                    Object o = JAXBUtils.toObject(StreamUtils.toInputStream(s), genericClass, mediaType);
                    c.add(o);
                }
                return c;

            }
            else
            {
                return null;
            }
        }
        catch (InstantiationException e)
        {
            log.error("Error during translation, Caused by:" + e.getMessage() + ", returning null");
            return null;
        }
        catch (IllegalAccessException e)
        {
            log.error("Error during translation, Caused by:" + e.getMessage() + ", returning null");
            return null;
        }
    }

}
