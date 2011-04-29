/*
 * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.entity;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;

import com.impetus.kundera.api.Collection;

/**
 * Entity class for Email
 * @author amresh.singh
 */

@Entity
@Collection(name="emails", db="mongodbtest")
public class Email {
	
	@Id
	@Column(name="unique_id")
	private String uniqueId;
	
	@Column(name="from_email")
	private String from;
	
	@Column(name="to_email")
	private String to;
	
	@Column(name="subject_email")
	private String subject;
	
	@Column(name="body_email")
	private String body;
	
	@OneToMany (cascade={CascadeType.ALL}, fetch=FetchType.LAZY)
	private List<Attachment> attachments;	
	
	public Email() {
		
	}
	
	public String toString() {
		return "UniqueId: " + uniqueId
			+ "\tFrom:" + from 
			+ "\tTo: " + to
			+ "\tSubject: " + subject
			+ "\tBody: " + body;
	}	
	
	public void addAttachment(Attachment attchment) {
		if(this.attachments == null || this.attachments.isEmpty()) {
			this.attachments = new ArrayList<Attachment>();
		}		
		this.attachments.add(attchment);
	}
	
	/**
	 * @return the uniqueId
	 */
	public String getUniqueId() {
		return uniqueId;
	}

	/**
	 * @param uniqueId the uniqueId to set
	 */
	public void setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
	}

	/**
	 * @return the from
	 */
	public String getFrom() {
		return from;
	}
	/**
	 * @param from the from to set
	 */
	public void setFrom(String from) {
		this.from = from;
	}
	/**
	 * @return the to
	 */
	public String getTo() {
		return to;
	}
	/**
	 * @param to the to to set
	 */
	public void setTo(String to) {
		this.to = to;
	}
	/**
	 * @return the subject
	 */
	public String getSubject() {
		return subject;
	}
	/**
	 * @param subject the subject to set
	 */
	public void setSubject(String subject) {
		this.subject = subject;
	}
	/**
	 * @return the body
	 */
	public String getBody() {
		return body;
	}
	/**
	 * @param body the body to set
	 */
	public void setBody(String body) {
		this.body = body;
	}

	/**
	 * @return the attachments
	 */
	public List<Attachment> getAttachments() {
		return attachments;
	}

	/**
	 * @param attachments the attachments to set
	 */
	public void setAttachments(List<Attachment> attachments) {
		this.attachments = attachments;
	}

}
