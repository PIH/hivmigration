package org.pih.hivmigration.common;

/**
 * Represents the commonality between an intake and a followup encounter.
 */
public class ClinicalEncounter extends Encounter {

	private String examiningDoctor;
	private String recommendations;
	private String comments;
	private Boolean startFinancialAid;
	private Boolean continueFinancialAid;
	private String formVersion;
	private ResponsiblePerson responsiblePerson;

	public ClinicalEncounter() {}

	public String getExaminingDoctor() {
		return examiningDoctor;
	}

	public void setExaminingDoctor(String examiningDoctor) {
		this.examiningDoctor = examiningDoctor;
	}

	public String getRecommendations() {
		return recommendations;
	}

	public void setRecommendations(String recommendations) {
		this.recommendations = recommendations;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}

	public Boolean getStartFinancialAid() {
		return startFinancialAid;
	}

	public void setStartFinancialAid(Boolean startFinancialAid) {
		this.startFinancialAid = startFinancialAid;
	}

	public Boolean getContinueFinancialAid() {
		return continueFinancialAid;
	}

	public void setContinueFinancialAid(Boolean continueFinancialAid) {
		this.continueFinancialAid = continueFinancialAid;
	}

	public String getFormVersion() {
		return formVersion;
	}

	public void setFormVersion(String formVersion) {
		this.formVersion = formVersion;
	}

	public ResponsiblePerson getResponsiblePerson() {
		return responsiblePerson;
	}

	public void setResponsiblePerson(ResponsiblePerson responsiblePerson) {
		this.responsiblePerson = responsiblePerson;
	}
}
