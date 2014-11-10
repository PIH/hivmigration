package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.DurationUnit;

import java.util.Date;

public class SymptomGroup {

	private Symptom symptom;
	private Boolean symptomPresent;
	private Date symptomDate;
	private Integer duration;
	private DurationUnit durationUnit;
	private String symptomComment;

	public SymptomGroup() {}

	public Symptom getSymptom() {
		return symptom;
	}

	public void setSymptom(Symptom symptom) {
		this.symptom = symptom;
	}

	public Boolean getSymptomPresent() {
		return symptomPresent;
	}

	public void setSymptomPresent(Boolean symptomPresent) {
		this.symptomPresent = symptomPresent;
	}

	public Date getSymptomDate() {
		return symptomDate;
	}

	public void setSymptomDate(Date symptomDate) {
		this.symptomDate = symptomDate;
	}

	public Integer getDuration() {
		return duration;
	}

	public void setDuration(Integer duration) {
		this.duration = duration;
	}

	public DurationUnit getDurationUnit() {
		return durationUnit;
	}

	public void setDurationUnit(DurationUnit durationUnit) {
		this.durationUnit = durationUnit;
	}

	public String getSymptomComment() {
		return symptomComment;
	}

	public void setSymptomComment(String symptomComment) {
		this.symptomComment = symptomComment;
	}
}
