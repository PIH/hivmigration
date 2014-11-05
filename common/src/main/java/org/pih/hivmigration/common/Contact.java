package org.pih.hivmigration.common;

import java.util.Date;

public class Contact {

	private String contactName;
	private String relationship;
	private Date birthDate;
	private Integer age;
	private String hivStatus;
	private Boolean followedInAClinicForHivCare;
	private String nameOfClinic;
	private Boolean referredForHivTest;
	private String nameOfReferralClinic;
	private Boolean deceased;

	public Contact() {}

	public String getContactName() {
		return contactName;
	}

	public void setContactName(String contactName) {
		this.contactName = contactName;
	}

	public String getRelationship() {
		return relationship;
	}

	public void setRelationship(String relationship) {
		this.relationship = relationship;
	}

	public Date getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public String getHivStatus() {
		return hivStatus;
	}

	public void setHivStatus(String hivStatus) {
		this.hivStatus = hivStatus;
	}

	public Boolean getFollowedInAClinicForHivCare() {
		return followedInAClinicForHivCare;
	}

	public void setFollowedInAClinicForHivCare(Boolean followedInAClinicForHivCare) {
		this.followedInAClinicForHivCare = followedInAClinicForHivCare;
	}

	public String getNameOfClinic() {
		return nameOfClinic;
	}

	public void setNameOfClinic(String nameOfClinic) {
		this.nameOfClinic = nameOfClinic;
	}

	public Boolean getReferredForHivTest() {
		return referredForHivTest;
	}

	public void setReferredForHivTest(Boolean referredForHivTest) {
		this.referredForHivTest = referredForHivTest;
	}

	public String getNameOfReferralClinic() {
		return nameOfReferralClinic;
	}

	public void setNameOfReferralClinic(String nameOfReferralClinic) {
		this.nameOfReferralClinic = nameOfReferralClinic;
	}

	public Boolean getDeceased() {
		return deceased;
	}

	public void setDeceased(Boolean deceased) {
		this.deceased = deceased;
	}
}
