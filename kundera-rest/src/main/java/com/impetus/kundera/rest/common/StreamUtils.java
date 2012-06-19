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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Utilities for converting InputStream to various forms and vice versa
 * @author amresh.singh
 */
public class StreamUtils
{
    
    /**
     * Converts Input stream to String
     * @param is
     * @return
     * @throws IOException
     */
    public static String toString(InputStream is) {
        String output = new String();
        try
        {
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            for(String line = br.readLine(); line != null; line = br.readLine()) 
              output += line;
        }
        catch (IOException e)
        {
           return null;
        }
        return output;
    }
    
    /**
     * Converts String to Input Stream
     * @param s
     * @return
     */
    public static InputStream toInputStream(String s) {
        InputStream is = new ByteArrayInputStream(s.getBytes());
        return is;
    }

}
