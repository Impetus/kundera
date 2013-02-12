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
package com.impetus.client.neo4j.imdb.composite;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * Class Holding Identity attributes for movies
 * @author amresh.singh
 */
@Embeddable
public class MovieId
{
    @Column(name="CERTIFICATION")
    private char certification;
    
    @Column(name="SERIAL_NUMBER")
    private long serialNumber;
    
    

    public MovieId()
    {        
    }

    /**
     * @param certification
     * @param serialNumber
     */
    public MovieId(char certification, long serialNumber)
    {
        super();
        this.certification = certification;
        this.serialNumber = serialNumber;
    }

    /**
     * @return the certification
     */
    public char getCertification()
    {
        return certification;
    }

    /**
     * @param certification the certification to set
     */
    public void setCertification(char certification)
    {
        this.certification = certification;
    }

    /**
     * @return the serialNumber
     */
    public long getSerialNumber()
    {
        return serialNumber;
    }

    /**
     * @param serialNumber the serialNumber to set
     */
    public void setSerialNumber(long serialNumber)
    {
        this.serialNumber = serialNumber;
    }
    
    

}
