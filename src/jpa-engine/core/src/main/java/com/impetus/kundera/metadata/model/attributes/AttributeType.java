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
package com.impetus.kundera.metadata.model.attributes;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.impetus.kundera.gis.geometry.Point;

/**
 * Attribute type.
 * 
 * @author Kuldeep.Mishra
 *
 */
public enum AttributeType
{
    ENUM, LIST, SET, MAP, POINT, PRIMITIVE;

    public static AttributeType getType(Class javaType)
    {
        AttributeType type = null;
        if (javaType.isAssignableFrom(List.class))
        {
            type = LIST;
        }
        else if (javaType.isAssignableFrom(Map.class))
        {
            type = MAP;
        }
        else if (javaType.isAssignableFrom(Set.class))
        {
            type = SET;
        }
        else if (javaType.isEnum())
        {
            type = ENUM;
        }
        else if (javaType.isAssignableFrom(Point.class))
        {
            type = POINT;
        }
        else
        {
            type = PRIMITIVE;
        }
        return type;
    }
}
