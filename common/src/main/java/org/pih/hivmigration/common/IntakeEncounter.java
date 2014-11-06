package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.WhoStagingCriteria;

import java.util.Date;
import java.util.List;

public class IntakeEncounter extends ClinicalEncounter {

	private String address;
	private String previousDiagnoses;
	private Boolean hospitalizedAtDiagnosis;

	private List<Allergy> allergies;
	private List<Contact> contacts;
	private List<Diagnosis> diagnoses;
	private List<PreviousTreatment> previousTreatments;
	private SocioeconomicData socioeconomicData;
	private HivStatusData hivStatusData;
	private List<SystemStatus> systemStatuses;
	private List<WhoStagingCriteria> whoStagingCriteria;

	public IntakeEncounter() {}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getPreviousDiagnoses() {
		return previousDiagnoses;
	}

	public void setPreviousDiagnoses(String previousDiagnoses) {
		this.previousDiagnoses = previousDiagnoses;
	}

	public Boolean getHospitalizedAtDiagnosis() {
		return hospitalizedAtDiagnosis;
	}

	public void setHospitalizedAtDiagnosis(Boolean hospitalizedAtDiagnosis) {
		this.hospitalizedAtDiagnosis = hospitalizedAtDiagnosis;
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

	public List<SystemStatus> getSystemStatuses() {
		return systemStatuses;
	}

	public void setSystemStatuses(List<SystemStatus> systemStatuses) {
		this.systemStatuses = systemStatuses;
	}

	public List<WhoStagingCriteria> getWhoStagingCriteria() {
		return whoStagingCriteria;
	}

	public void setWhoStagingCriteria(List<WhoStagingCriteria> whoStagingCriteria) {
		this.whoStagingCriteria = whoStagingCriteria;
	}
}
