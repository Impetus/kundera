package com.impetus.kundera.tests.crossdatastore.useraddress.entities;

import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

@Entity
@Table(name = "PERSONNEL", schema = "KunderaTests")
public class PersonnelBi1ToM {
	@Id
	@Column(name = "PERSON_ID")
	private String personId;

	@Column(name = "PERSON_NAME")
	private String personName;
	
	@OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy="person")	
	private Set<HabitatBi1ToM> addresses;

	public String getPersonId() {
		return personId;
	}

	public String getPersonName() {
		return personName;
	}

	public void setPersonName(String personName) {
		this.personName = personName;
	}

	public void setPersonId(String personId) {
		this.personId = personId;
	}
	
	public Set<HabitatBi1ToM> getAddresses() {
		return addresses;
	}

	public void setAddresses(Set<HabitatBi1ToM> addresses) {
		this.addresses = addresses;
	}
}
