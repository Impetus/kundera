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

import java.io.IOException;
import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility for converting objects into XML and vice versa 
 * @author amresh.singh
 */
public class JAXBUtils
{
    private static Log log = LogFactory.getLog(JAXBUtils.class);
    
    /**
     * Converts <code>InputStream</code> to Object using JAXB
     * @param str
     * @param objectClass
     * @return
     */
    public static Object toObject(InputStream is, Class<?> objectClass) {
        Object output = null;
        try
        {
            JAXBContext jaxbContext = JAXBContext.newInstance(objectClass);
            
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            
            output = objectClass.newInstance();           
            output = jaxbUnmarshaller.unmarshal(is);
            
        }
        catch (JAXBException e)
        {
            log.warn("Error while converting String to Object using JAXB:" + e.getMessage());
            return null;
        }
        catch (InstantiationException e)
        {
            log.warn("Error while converting String to Object using JAXB:" + e.getMessage());
            return null;
        }
        catch (IllegalAccessException e)
        {
            log.warn("Error while converting String to Object using JAXB:" + e.getMessage());
            return null;
        }
        return output;
    }

}
