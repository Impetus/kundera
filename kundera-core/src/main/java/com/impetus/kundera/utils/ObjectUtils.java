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
package com.impetus.kundera.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Provides utility methods for operation on objects
 * 
 * @author amresh.singh
 */
public class ObjectUtils
{
    /**
     * Provides a deep clone serializing/de-serializng <code>objectToCopy</code>
     * @param objectToCopy The object to be cloned
     * @return The cloned object
     */
    public static final Object deepCopy(Object objectToCopy)
    {
        
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream(100);
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(objectToCopy);                
                byte bytes[] = bos.toByteArray();
                oos.close();
                
                
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                Object copy = ois.readObject();
                ois.close();
                return copy;
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            catch (ClassNotFoundException e)
            {
                e.printStackTrace();
            }
            return null;
        
    }

}
