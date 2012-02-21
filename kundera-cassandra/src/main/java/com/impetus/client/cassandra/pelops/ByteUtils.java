/*******************************************************************************
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
package com.impetus.client.cassandra.pelops;

import com.impetus.kundera.Constants;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.scale7.cassandra.pelops.Bytes;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * All methods in this utilities class take UUIDs into consideration when
 * converting.
 * @author kcarlson
 */
public class ByteUtils
{

    /**
     * Converts a string to a Bytes object taking into consideration that the
     * string may be a UUID.
     * @param str
     * @return 
     */
    public static Bytes stringToBytes(String str)
    {
        try
        {
            UUID uuid = UUID.fromString(str);
            return Bytes.fromUuid(uuid);
        }
        catch (IllegalArgumentException ex)
        {
            return Bytes.fromByteArray(str.getBytes());
        }
    }

    /**
     * Converts a Bytes object into a string taking into consideration that the
     * Bytes object may be a UUID.
     * @param bytes
     * @return 
     */
    public static String bytesToString(Bytes bytes)
    {
        try
        {
            UUID uuid = bytes.toUuid();
            return uuid.toString();
        }
        catch (IllegalStateException ex)
        {
            return Bytes.toUTF8(bytes.toByteArray());
        }
    }

    /**
     * Converts a byte array into a string taking into consideration that the
     * byte array may be a UUID.
     * @param byteArray
     * @return 
     */
    public static String byteArrayToString(byte[] byteArray)
    {
        return bytesToString(Bytes.fromByteArray(byteArray));
    }

    /**
     * COnverts a string to a byte array taking into consideration that the
     * string may be a UUID.
     * @param str
     * @return
     * @throws Exception 
     */
    public static byte[] stringToByteArray(String str) throws Exception
    {
        try
        {
            UUID uuid = UUID.fromString(((String) str));
            return Bytes.fromUuid(uuid).toByteArray();
        }
        catch (IllegalArgumentException ex)
        {
            try
            {
                return str.getBytes(Constants.ENCODING);
            }
            catch (UnsupportedEncodingException ex1)
            {
                throw new Exception(ex1.getMessage());
            }
        }
    }

    /**
     * Converts a string into a ByteBuffer object taking into consideration that the
     * string may be a UUID.
     * @param str
     * @return 
     */
    public static ByteBuffer stringToByteBuffer(String str)
    {
        try
        {
            UUID uuid = UUID.fromString(str);
            return ByteBuffer.wrap(Bytes.fromUuid(uuid).toByteArray());
        }
        catch (IllegalArgumentException ex)
        {
            return ByteBufferUtil.bytes(str);
        }
    }
}
