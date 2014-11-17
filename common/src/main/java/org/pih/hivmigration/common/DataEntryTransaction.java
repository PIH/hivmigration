package org.pih.hivmigration.common;

import java.util.Date;

/**
 * Represents a data entry transaction.  Does not imply an actual patient encounter, but simply the fact that data entry was done
 * to add or modify patient data at a particular time
 */
public class DataEntryTransaction {

	private Date entryDate;
	private User enteredBy;

	public DataEntryTransaction() {}

	public Date getEntryDate() {
		return entryDate;
	}

	public void setEntryDate(Date entryDate) {
		this.entryDate = entryDate;
	}

	public User getEnteredBy() {
		return enteredBy;
	}

	public void setEnteredBy(User enteredBy) {
		this.enteredBy = enteredBy;
	}
}
