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
 * Class Holding Identity attributes for actors 
 * @author amresh.singh
 */
@Embeddable
public class ActorId
{
    @Column(name="ACTOR_ID_PREFIX")
    private String prefix;
    
    @Column(name="ACTOR_ID_SUFFIX")
    private int suffix;
    
    public ActorId()
    {
    }

    /**
     * @param prefix
     * @param suffix
     */
    public ActorId(String prefix, int suffix)
    {
        super();
        this.prefix = prefix;
        this.suffix = suffix;
    }

    /**
     * @return the prefix
     */
    public String getPrefix()
    {
        return prefix;
    }

    /**
     * @param prefix the prefix to set
     */
    public void setPrefix(String prefix)
    {
        this.prefix = prefix;
    }

    /**
     * @return the suffix
     */
    public int getSuffix()
    {
        return suffix;
    }

    /**
     * @param suffix the suffix to set
     */
    public void setSuffix(int suffix)
    {
        this.suffix = suffix;
    }
    
    

}
