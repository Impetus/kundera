package com.impetus.kundera.tests.crossdatastore.useraddress.entities;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name = "PERSONNEL", schema = "KunderaTests")
public class PersonnelUniMTo1 {
	@Id
	@Column(name = "PERSON_ID")
	private String personId;

	@Column(name = "PERSON_NAME")
	private String personName;
	
	@ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
	@JoinColumn(name="ADDRESS_ID")
	private HabitatUniMTo1 address;

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

	public HabitatUniMTo1 getAddress() {
		return address;
	}

	public void setAddress(HabitatUniMTo1 address) {
		this.address = address;
	}

}
