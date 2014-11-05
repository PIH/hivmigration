package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.HivStatus;

import java.util.Date;

public class HivStatusData {

	private HivStatus status;
	private Date statusDate;
	private Boolean dateUnknown;
	private String testLocationCoded;
	private String testLocationNonCoded;
	private String entryDate;
	private User enteredBy;

	public HivStatusData() {}

	public HivStatus getStatus() {
		return status;
	}

	public void setStatus(HivStatus status) {
		this.status = status;
	}

	public Date getStatusDate() {
		return statusDate;
	}

	public void setStatusDate(Date statusDate) {
		this.statusDate = statusDate;
	}

	public Boolean getDateUnknown() {
		return dateUnknown;
	}

	public void setDateUnknown(Boolean dateUnknown) {
		this.dateUnknown = dateUnknown;
	}

	public String getTestLocationCoded() {
		return testLocationCoded;
	}

	public void setTestLocationCoded(String testLocationCoded) {
		this.testLocationCoded = testLocationCoded;
	}

	public String getTestLocationNonCoded() {
		return testLocationNonCoded;
	}

	public void setTestLocationNonCoded(String testLocationNonCoded) {
		this.testLocationNonCoded = testLocationNonCoded;
	}

	public String getEntryDate() {
		return entryDate;
	}

	public void setEntryDate(String entryDate) {
		this.entryDate = entryDate;
	}

	public User getEnteredBy() {
		return enteredBy;
	}

	public void setEnteredBy(User enteredBy) {
		this.enteredBy = enteredBy;
	}
}
