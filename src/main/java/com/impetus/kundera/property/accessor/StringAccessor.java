/*
 * Copyright 2010 Impetus Infotech.
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
package com.impetus.kundera.property.accessor;

import com.impetus.kundera.Constants;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class StringAccessor.
 * 
 * @author animesh.kumar
 */
public class StringAccessor implements PropertyAccessor<String> {

    /* @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[]) */
    @Override
    public final String fromBytes(byte[] bytes) throws PropertyAccessException {
        try {
            return new String(bytes, Constants.ENCODING);
        } catch (Exception e) {
            e.printStackTrace();
            throw new PropertyAccessException(e.getMessage());
        }
    }

    /* @see com.impetus.kundera.property.PropertyAccessor#toBytes(java.lang.Object) */
    @Override
    public final byte[] toBytes(Object s) throws PropertyAccessException {
        try {
            return ((String) s).getBytes(Constants.ENCODING);
        } catch (Exception e) {
            throw new PropertyAccessException(e.getMessage());
        }
    }

    /* @see com.impetus.kundera.property.PropertyAccessor#toString(java.lang.Object) */
    @Override
    public final String toString(Object object) {
        return (String) object;
    }

}
