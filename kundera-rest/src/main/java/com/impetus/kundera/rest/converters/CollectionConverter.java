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

import com.impetus.kundera.rest.common.JAXBUtils;

/**
 * Converts a Collection object to XML/ JSON representation and vice-versa
 * @author amresh
 *
 */
public class CollectionConverter
{
    public static String toString(Collection<?> input, Class<?> genericClass, String mediaType) {
        StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>")
        .append("<").append(genericClass.getSimpleName().toLowerCase()).append("s>");
        for(Object obj : input) {
            String s = JAXBUtils.toString(genericClass, obj, mediaType);
            
            if(s.startsWith("<?xml")) {
                s = s.substring(s.indexOf(">") + 1, s.length());
            }            
            sb.append(s);
        }
        sb.append("<").append(genericClass.getSimpleName().toLowerCase()).append("s>");         
        return sb.toString();
    }

}
