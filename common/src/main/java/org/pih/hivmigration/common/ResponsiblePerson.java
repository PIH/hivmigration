package org.pih.hivmigration.common;

public class ResponsiblePerson {

	private String firstName;
	private String lastName;
	private String pihId;
	private String relationship;

	public ResponsiblePerson() {}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getPihId() {
		return pihId;
	}

	public void setPihId(String pihId) {
		this.pihId = pihId;
	}

	public String getRelationship() {
		return relationship;
	}

	public void setRelationship(String relationship) {
		this.relationship = relationship;
	}
}
