/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.secondarytable;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * The Class EmbeddedEntity.
 * 
 * @author Pragalbh Garg
 */
@Embeddable
public class EmbeddedEntity
{

    /** The email id. */
    @Column(name = "EMAIL_ID", table = "HBASE_SECONDARY_TABLE")
    private String emailId;

    /** The phone no. */
    @Column(name = "PHONE_NO")
    private long phoneNo;

    /**
     * Gets the email id.
     * 
     * @return the email id
     */
    public String getEmailId()
    {
        return emailId;
    }

    /**
     * Sets the email id.
     * 
     * @param emailId
     *            the new email id
     */
    public void setEmailId(String emailId)
    {
        this.emailId = emailId;
    }

    /**
     * Gets the phone no.
     * 
     * @return the phone no
     */
    public long getPhoneNo()
    {
        return phoneNo;
    }

    /**
     * Sets the phone no.
     * 
     * @param phoneNo
     *            the new phone no
     */
    public void setPhoneNo(long phoneNo)
    {
        this.phoneNo = phoneNo;
    }

}
