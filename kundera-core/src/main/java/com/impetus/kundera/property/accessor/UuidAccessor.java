/*******************************************************************************
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
package com.impetus.kundera.property.accessor;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import java.util.UUID;
import org.scale7.cassandra.pelops.Bytes;

/**
 *
 * @author kcarlson
 */
public class UuidAccessor implements PropertyAccessor<UUID>
{

    @Override
    public UUID fromBytes(byte[] bytes) throws PropertyAccessException
    {
        try
        {
            return Bytes.fromByteArray(bytes).toUuid();
        }
        catch (Exception e)
        {
            throw new PropertyAccessException(e.getMessage());
        }
    }

    @Override
    public byte[] toBytes(Object object) throws PropertyAccessException
    {
        try
        {
            UUID uuid = UUID.fromString(((String)object));
            return Bytes.fromUuid(uuid).toByteArray();
        }
        catch (Exception e)
        {
            throw new PropertyAccessException(e.getMessage());
        }
    }

    @Override
    public String toString(Object object)
    {
        return ((UUID)object).toString();
    }

    @Override
    public UUID fromString(String s) throws PropertyAccessException
    {
        try
        {
            return UUID.fromString(s);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new PropertyAccessException(e.getMessage());
        }
    }
    
}
