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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;

/**
 * @author amresh
 * 
 */
public class ListMessageBodyProvider implements MessageBodyWriter<ArrayList<Object>>
{

    @Override
    public boolean isWriteable(Class<?> paramClass, Type paramType, Annotation[] paramArrayOfAnnotation,
            MediaType paramMediaType)
    {
        /*
         * // Ensure that we're handling only List<GPSTrackerCollection>
         * objects. boolean isWritable; if
         * (List.class.isAssignableFrom(paramClass) && paramType instanceof
         * ParameterizedType) { ParameterizedType parameterizedType =
         * (ParameterizedType) paramType; Type[] actualTypeArgs =
         * (parameterizedType.getActualTypeArguments()); isWritable =
         * (actualTypeArgs.length == 1 && actualTypeArgs[0]
         * .equals(GPSTrackerCollection.class)); } else { isWritable = false; }
         * 
         * return isWritable;
         */
        return true;
    }

    @Override
    public long getSize(ArrayList<Object> paramT, Class<?> paramClass, Type paramType, Annotation[] paramArrayOfAnnotation,
            MediaType paramMediaType)
    {
        return -1;
    }

    @Override
    public void writeTo(ArrayList<Object> paramT, Class<?> paramClass, Type paramType, Annotation[] paramArrayOfAnnotation,
            MediaType paramMediaType, MultivaluedMap<String, Object> paramMultivaluedMap, OutputStream paramOutputStream)
            throws IOException, WebApplicationException
    {
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(paramOutputStream));
        String ts = null;
        Iterator<Object> i = paramT.iterator();
        while (i.hasNext())
        {
            ts += i.next().toString();
        }
        bw.write(ts);
        bw.flush();
    }

}
