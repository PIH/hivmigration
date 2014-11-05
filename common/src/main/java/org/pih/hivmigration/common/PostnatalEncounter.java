package org.pih.hivmigration.common;

public class PostnatalEncounter extends Encounter {

	private Integer pregnancyId; // Legacy id for later re-association with Pregnancies if needed
	private String childHivTestType;
	private String childHivTestResult;
	private String childStatus;

	public PostnatalEncounter() {}

	public Integer getPregnancyId() {
		return pregnancyId;
	}

	public void setPregnancyId(Integer pregnancyId) {
		this.pregnancyId = pregnancyId;
	}

	public String getChildHivTestType() {
		return childHivTestType;
	}

	public void setChildHivTestType(String childHivTestType) {
		this.childHivTestType = childHivTestType;
	}

	public String getChildHivTestResult() {
		return childHivTestResult;
	}

	public void setChildHivTestResult(String childHivTestResult) {
		this.childHivTestResult = childHivTestResult;
	}

	public String getChildStatus() {
		return childStatus;
	}

	public void setChildStatus(String childStatus) {
		this.childStatus = childStatus;
	}
}
