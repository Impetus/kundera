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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PrePersist;

import com.impetus.kundera.api.ColumnFamily;
import com.impetus.kundera.api.Index;

/**
 * @author animesh.kumar
 *
 */
@Entity
@ColumnFamily(family="Person")
@Index(index=false)
public class Person implements Serializable {
	
	@Id
	private String username;
	
	@Column
	private String password;
	
	@OneToOne (cascade={CascadeType.PERSIST, CascadeType.REMOVE})
	private Profile profile;
	
	@OneToOne (cascade={CascadeType.PERSIST, CascadeType.MERGE, CascadeType.REMOVE})
	private Profile publicProfile;

	
	@OneToMany (cascade={CascadeType.ALL}) //(targetEntity=Post.class)
	private Set<Post> post = new HashSet<Post>();
	

	public Person() {

	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Profile getProfile() {
		return profile;
	}

	public void setProfile(Profile profile) {
		this.profile = profile;
	}

	public Profile getPublicProfile() {
		return publicProfile;
	}

	public void setPublicProfile(Profile publicProfile) {
		this.publicProfile = publicProfile;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Person [username=");
		builder.append(username);
		builder.append(", password=");
		builder.append(password);
		builder.append(", post=");
		builder.append(post);
		builder.append(", profile=");
		builder.append(profile);
		builder.append(", publicProfile=");
		builder.append(publicProfile);
		builder.append("]");
		return builder.toString();
	}


	@PrePersist
	public void pre () {
		System.out.println ("PRE PERSIST >> " + this);
	}

	/**
	 * @param e
	 * @return
	 * @see java.util.Set#add(java.lang.Object)
	 */
	public boolean addPost(Post e) {
		return post.add(e);
	}
	
	public int size() {
		return post.size();
	}
}
