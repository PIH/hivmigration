package org.pih.hivmigration.common;

import java.util.Date;

public class Diagnosis {

	private String diagnosisCoded;
	private String diagnosisNonCoded;
	private Boolean present;
	private Date diagnosisDate;
	private String diagnosisComments;

	public Diagnosis() {}

	public String getDiagnosisCoded() {
		return diagnosisCoded;
	}

	public void setDiagnosisCoded(String diagnosisCoded) {
		this.diagnosisCoded = diagnosisCoded;
	}

	public String getDiagnosisNonCoded() {
		return diagnosisNonCoded;
	}

	public void setDiagnosisNonCoded(String diagnosisNonCoded) {
		this.diagnosisNonCoded = diagnosisNonCoded;
	}

	public Boolean getPresent() {
		return present;
	}

	public void setPresent(Boolean present) {
		this.present = present;
	}

	public Date getDiagnosisDate() {
		return diagnosisDate;
	}

	public void setDiagnosisDate(Date diagnosisDate) {
		this.diagnosisDate = diagnosisDate;
	}

	public String getDiagnosisComments() {
		return diagnosisComments;
	}

	public void setDiagnosisComments(String diagnosisComments) {
		this.diagnosisComments = diagnosisComments;
	}
}
