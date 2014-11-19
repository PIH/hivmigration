package org.pih.hivmigration.common;

import java.util.List;

/**
 * Represents a nutritional evaluation encounter, during the anlap program food study
 */
public class NutritionalEvaluationEncounter extends Encounter {

	private List<LabTestResult> labResults;
	private Double weight;
	private Double height;

	public NutritionalEvaluationEncounter() {}

	public List<LabTestResult> getLabResults() {
		return labResults;
	}

	public void setLabResults(List<LabTestResult> labResults) {
		this.labResults = labResults;
	}

	public Double getWeight() {
		return weight;
	}

	public void setWeight(Double weight) {
		this.weight = weight;
	}

	public Double getHeight() {
		return height;
	}

	public void setHeight(Double height) {
		this.height = height;
	}
}
