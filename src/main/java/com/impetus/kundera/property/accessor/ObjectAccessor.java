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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;

/**
 * The Class ObjectAccessor.
 * 
 * @author animesh.kumar
 */
public class ObjectAccessor implements PropertyAccessor<Object> {

    @Override
    public final Object fromBytes(byte[] bytes) throws PropertyAccessException {
        try {
            ObjectInputStream ois;
            ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            Object o = ois.readObject();
            ois.close();
            return o;
        } catch (Exception ioe) {
            throw new PropertyAccessException(ioe.getMessage());
        }
    }

    @Override
    public final byte[] toBytes(Object o) throws PropertyAccessException {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(o);
            oos.close();
            return baos.toByteArray();
        } catch (Exception ioe) {
            throw new PropertyAccessException(ioe.getMessage());
        }
    }

	@Override
	public final String toString(Object object) {
		return object.toString();
	}

}
