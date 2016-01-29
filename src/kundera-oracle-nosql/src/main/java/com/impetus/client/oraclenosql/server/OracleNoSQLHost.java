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
package com.impetus.client.oraclenosql.server;

import java.util.Properties;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.impetus.kundera.service.Host;

/**
 * Cassandra Host configuration.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class OracleNoSQLHost implements Host
{

    public static final int DEFAULT_PORT = 5000;

    private String host;

    private int port;

    private String userName;

    private String password;
    
   

    
    public OracleNoSQLHost(String host)
    {
        this.host = host;
        this.port = DEFAULT_PORT;
    }

    public OracleNoSQLHost(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    @Override
    public String getHost()
    {
        return host;
    }

    @Override
    public int getPort()
    {
        return port;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof OracleNoSQLHost))
        {
            return false;
        }
        OracleNoSQLHost other = (OracleNoSQLHost) obj;
        return other.host.equals(this.host) && other.port == this.port;
    }

    @Override
    public int hashCode()
    {
        StringBuilder builder = new StringBuilder(host);
        builder.append(port);
        return HashCodeBuilder.reflectionHashCode(builder);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(host);
        builder.append(":");
        builder.append(port);
        return builder.toString();
    }




    @Override
    public String getUser()
    {
        return this.userName;
    }

    @Override
    public String getPassword()
    {
        return this.password;
    }

    public void setUserName(String userName)
    {
        this.userName = userName;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    @Override
    public int getMaxTotal()
    {
        
        return 0;
    }

    @Override
    public int getMaxActive()
    {
        
        return 0;
    }

    @Override
    public int getMaxIdle()
    {
       
        return 0;
    }

    @Override
    public int getMinIdle()
    {
       
        return 0;
    }

    @Override
    public boolean isRetryHost()
    {
      
        return false;
    }

 
  
}
