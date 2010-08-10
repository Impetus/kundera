/*
 * Copyright 2010 Impetus Infotech.
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

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.PrePersist;

import com.impetus.kundera.api.ColumnFamily;

@Entity
@ColumnFamily(family = "Profile")
public class Profile {

	@Id
	private String profileId;

	@Column
	private String address;

	@Column
	private String website;

	@Column
	private String blog;

	@OneToOne (cascade={CascadeType.ALL})
	private Person person;

	public Profile() {

	}

	public Person getPerson() {
		return person;
	}

	public void setPerson(Person person) {
		this.person = person;
	}

	public String getProfileId() {
		return profileId;
	}

	public void setProfileId(String profileId) {
		this.profileId = profileId;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getWebsite() {
		return website;
	}

	public void setWebsite(String website) {
		this.website = website;
	}

	public String getBlog() {
		return blog;
	}

	public void setBlog(String blog) {
		this.blog = blog;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Profile [profileId=");
		builder.append(profileId);
		builder.append(", address=");
		builder.append(address);
		builder.append(", blog=");
		builder.append(blog);
		builder.append(", person=");
//		builder.append(person.getUsername());
		builder.append(", website=");
		builder.append(website);
		builder.append("]");
		return builder.toString();
	}

	@PrePersist
	public void pre () {
		System.out.println ("PRE PERSIST >> " + this);
	}

}
