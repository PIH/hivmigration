package org.pih.hivmigration.common;

import java.util.Date;
import java.util.List;

public class IntakeEncounter extends Encounter {

	private String comments; // From hiv_encounters.comments

	private List<Allergy> allergies;
	private List<Contact> contacts;
	private List<Diagnosis> diagnoses;
	private List<PreviousTreatment> previousTreatments;
	private SocioeconomicData socioeconomicData;
	private HivStatusData hivStatusData;

	public IntakeEncounter() {}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}

	public List<Allergy> getAllergies() {
		return allergies;
	}

	public void setAllergies(List<Allergy> allergies) {
		this.allergies = allergies;
	}

	public List<Contact> getContacts() {
		return contacts;
	}

	public void setContacts(List<Contact> contacts) {
		this.contacts = contacts;
	}

	public List<Diagnosis> getDiagnoses() {
		return diagnoses;
	}

	public void setDiagnoses(List<Diagnosis> diagnoses) {
		this.diagnoses = diagnoses;
	}

	public List<PreviousTreatment> getPreviousTreatments() {
		return previousTreatments;
	}

	public void setPreviousTreatments(List<PreviousTreatment> previousTreatments) {
		this.previousTreatments = previousTreatments;
	}

	public SocioeconomicData getSocioeconomicData() {
		return socioeconomicData;
	}

	public void setSocioeconomicData(SocioeconomicData socioeconomicData) {
		this.socioeconomicData = socioeconomicData;
	}

	public HivStatusData getHivStatusData() {
		return hivStatusData;
	}

	public void setHivStatusData(HivStatusData hivStatusData) {
		this.hivStatusData = hivStatusData;
	}
}
