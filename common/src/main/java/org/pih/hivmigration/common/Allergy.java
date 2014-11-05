package org.pih.hivmigration.common;

import java.util.Date;

public class Allergy {

	private String allergen;
	private Date allergyDate;
	private String typeOfReaction;

	public Allergy() {
	}

	public String getAllergen() {
		return allergen;
	}

	public void setAllergen(String allergen) {
		this.allergen = allergen;
	}

	public Date getAllergyDate() {
		return allergyDate;
	}

	public void setAllergyDate(Date allergyDate) {
		this.allergyDate = allergyDate;
	}

	public String getTypeOfReaction() {
		return typeOfReaction;
	}

	public void setTypeOfReaction(String typeOfReaction) {
		this.typeOfReaction = typeOfReaction;
	}
}
