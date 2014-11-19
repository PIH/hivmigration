package org.pih.hivmigration.common;

import java.util.List;

/**
 * Represents a record of a patient receiving nutritional assistance
 */
public class FoodSupportEncounter extends Encounter {

	private String dateReceived;

	public FoodSupportEncounter() {}

	public String getDateReceived() {
		return dateReceived;
	}

	public void setDateReceived(String dateReceived) {
		this.dateReceived = dateReceived;
	}
}
