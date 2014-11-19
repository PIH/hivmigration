package org.pih.hivmigration.common;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Patient {

	private Integer patientId;
	private String pihId;
	private String nifId;
	private String nationalId;
	private String firstName;
	private String firstName2;
	private String lastName;
	private String gender;
	private Date birthDate;
	private Boolean birthdateEstimated;
	private String phoneNumber;
	private String birthplace;
	private String accompagnateur;
	private User patientCreatedBy;
	private Date patientCreatedDate;

	private PamEnrollment pamEnrollment;
	private List<Address> addresses;

	private List<IntakeEncounter> intakeEncounters;
	private List<FollowupEncounter> followupEncounters;
	private List<PatientContactEncounter> patientContactEncounters;
	private List<CervicalCancerEncounter> cervicalCancerEncounters;
	private List<NutritionalEvaluationEncounter> nutritionalEvaluationEncounters;
	private List<LabResultEncounter> labResultEncounters;
	private List<FoodSupportEncounter> foodSupportEncounters;
	private List<AccompagnateurMedicationPickup> accompagnateurMedicationPickups;
	private List<Note> notes;
	private List<PregnancyDataEntryTransaction> pregnancyDataEntryTransactions;
	private List<Pregnancy> pregnancies;
	private List<PostnatalEncounter> postnatalEncounters;

	public Patient() {}

	public Integer getPatientId() {
		return patientId;
	}

	public void setPatientId(Integer patientId) {
		this.patientId = patientId;
	}

	public String getPihId() {
		return pihId;
	}

	public void setPihId(String pihId) {
		this.pihId = pihId;
	}

	public String getNifId() {
		return nifId;
	}

	public void setNifId(String nifId) {
		this.nifId = nifId;
	}

	public String getNationalId() {
		return nationalId;
	}

	public void setNationalId(String nationalId) {
		this.nationalId = nationalId;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getFirstName2() {
		return firstName2;
	}

	public void setFirstName2(String firstName2) {
		this.firstName2 = firstName2;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public Date getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate;
	}

	public Boolean getBirthdateEstimated() {
		return birthdateEstimated;
	}

	public void setBirthdateEstimated(Boolean birthdateEstimated) {
		this.birthdateEstimated = birthdateEstimated;
	}

	public String getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	public String getBirthplace() {
		return birthplace;
	}

	public void setBirthplace(String birthplace) {
		this.birthplace = birthplace;
	}

	public String getAccompagnateur() {
		return accompagnateur;
	}

	public void setAccompagnateur(String accompagnateur) {
		this.accompagnateur = accompagnateur;
	}

	public User getPatientCreatedBy() {
		return patientCreatedBy;
	}

	public void setPatientCreatedBy(User patientCreatedBy) {
		this.patientCreatedBy = patientCreatedBy;
	}

	public Date getPatientCreatedDate() {
		return patientCreatedDate;
	}

	public void setPatientCreatedDate(Date patientCreatedDate) {
		this.patientCreatedDate = patientCreatedDate;
	}

	public PamEnrollment getPamEnrollment() {
		return pamEnrollment;
	}

	public void setPamEnrollment(PamEnrollment pamEnrollment) {
		this.pamEnrollment = pamEnrollment;
	}

	public List<Address> getAddresses() {
		if (addresses == null) {
			addresses = new ArrayList<Address>();
		}
		return addresses;
	}

	public void setAddresses(List<Address> addresses) {
		this.addresses = addresses;
	}

	public void addAddress(Address address) {
		getAddresses().add(address);
	}

	public List<IntakeEncounter> getIntakeEncounters() {
		return intakeEncounters;
	}

	public void setIntakeEncounters(List<IntakeEncounter> intakeEncounters) {
		this.intakeEncounters = intakeEncounters;
	}

	public List<FollowupEncounter> getFollowupEncounters() {
		return followupEncounters;
	}

	public void setFollowupEncounters(List<FollowupEncounter> followupEncounters) {
		this.followupEncounters = followupEncounters;
	}

	public List<PatientContactEncounter> getPatientContactEncounters() {
		return patientContactEncounters;
	}

	public void setPatientContactEncounters(List<PatientContactEncounter> patientContactEncounters) {
		this.patientContactEncounters = patientContactEncounters;
	}

	public List<CervicalCancerEncounter> getCervicalCancerEncounters() {
		return cervicalCancerEncounters;
	}

	public void setCervicalCancerEncounters(List<CervicalCancerEncounter> cervicalCancerEncounters) {
		this.cervicalCancerEncounters = cervicalCancerEncounters;
	}

	public List<NutritionalEvaluationEncounter> getNutritionalEvaluationEncounters() {
		return nutritionalEvaluationEncounters;
	}

	public void setNutritionalEvaluationEncounters(List<NutritionalEvaluationEncounter> nutritionalEvaluationEncounters) {
		this.nutritionalEvaluationEncounters = nutritionalEvaluationEncounters;
	}

	public List<LabResultEncounter> getLabResultEncounters() {
		return labResultEncounters;
	}

	public void setLabResultEncounters(List<LabResultEncounter> labResultEncounters) {
		this.labResultEncounters = labResultEncounters;
	}

	public List<FoodSupportEncounter> getFoodSupportEncounters() {
		return foodSupportEncounters;
	}

	public void setFoodSupportEncounters(List<FoodSupportEncounter> foodSupportEncounters) {
		this.foodSupportEncounters = foodSupportEncounters;
	}

	public List<AccompagnateurMedicationPickup> getAccompagnateurMedicationPickups() {
		return accompagnateurMedicationPickups;
	}

	public void setAccompagnateurMedicationPickups(List<AccompagnateurMedicationPickup> accompagnateurMedicationPickups) {
		this.accompagnateurMedicationPickups = accompagnateurMedicationPickups;
	}

	public List<Note> getNotes() {
		return notes;
	}

	public void setNotes(List<Note> notes) {
		this.notes = notes;
	}

	public List<PregnancyDataEntryTransaction> getPregnancyDataEntryTransactions() {
		return pregnancyDataEntryTransactions;
	}

	public void setPregnancyDataEntryTransactions(List<PregnancyDataEntryTransaction> pregnancyDataEntryTransactions) {
		this.pregnancyDataEntryTransactions = pregnancyDataEntryTransactions;
	}

	public List<Pregnancy> getPregnancies() {
		if (pregnancies == null) {
			pregnancies = new ArrayList<Pregnancy>();
		}
		return pregnancies;
	}

	public void setPregnancies(List<Pregnancy> pregnancies) {
		this.pregnancies = pregnancies;
	}

	public void addPregnancy(Pregnancy pregnancy) {
		getPregnancies().add(pregnancy);
	}

	public List<PostnatalEncounter> getPostnatalEncounters() {
		if (postnatalEncounters == null) {
			postnatalEncounters = new ArrayList<PostnatalEncounter>();
		}
		return postnatalEncounters;
	}

	public void setPostnatalEncounters(List<PostnatalEncounter> postnatalEncounters) {
		this.postnatalEncounters = postnatalEncounters;
	}

	public void addPostnatalEncounter(PostnatalEncounter encounter) {
		getPostnatalEncounters().add(encounter);
	}
}
