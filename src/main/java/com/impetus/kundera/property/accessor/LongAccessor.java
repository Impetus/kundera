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

import java.nio.ByteBuffer;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class LongAccessor.
 * 
 * @author animesh.kumar
 */
public class LongAccessor implements PropertyAccessor<Long> {

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public final Long fromBytes(byte[] bytes) throws PropertyAccessException {
        if (bytes.length != 8) {
            throw new PropertyAccessException("must be 8 bytes");
        }
        return (ByteBuffer.wrap(bytes).getLong());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#toBytes(java.lang.Object)
     */
    @Override
    public final byte[] toBytes(Object object) {
        Long l = (Long) object;
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(l);
        return buffer.array();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.property.PropertyAccessor#toString(java.lang.Object)
     */
    @Override
    public final String toString(Object object) {
        return object.toString();
    }
}
