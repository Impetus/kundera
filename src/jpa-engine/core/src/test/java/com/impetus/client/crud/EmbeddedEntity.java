/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.crud;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * 
 * @author Kuldeep.Mishra
 *
 */
@Embeddable
public class EmbeddedEntity
{
    @Column(name = "EMAIL_ID", table = "SECONDARY_TABLE")
    private String emailId;

    @Column(name = "PHONE_NO")
    private long phoneNo;

    public String getEmailId()
    {
        return emailId;
    }

    public void setEmailId(String emailId)
    {
        this.emailId = emailId;
    }

    public long getPhoneNo()
    {
        return phoneNo;
    }

    public void setPhoneNo(long phoneNo)
    {
        this.phoneNo = phoneNo;
    }

}
