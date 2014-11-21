package org.pih.hivmigration.common;

import java.util.Date;

/**
 * Represents a record af an accompagnateur picking up medications for a patient
 */
public class AccompagnateurMedicationPickup extends Encounter {

	@ObsName("accompagnateur_date_came_for_meds") private Date dateOfPickup;
	@ObsName("accompagnateur_date_of_next_visit") private Date dateOfNextVisit;
	@ObsName("accompagnateur_name") private String accompagnateurName;
	@ObsName("accompagnateur_picked_up_ctx") private Boolean pickedUpCtx;
	@ObsName("accompagnateur_picked_up_inh")  private Boolean pickedUpInh;
	@ObsName("accompagnateur_reason_for_not_coming") private String reasonForNotComing;

	public AccompagnateurMedicationPickup() {}

	public Date getDateOfPickup() {
		return dateOfPickup;
	}

	public void setDateOfPickup(Date dateOfPickup) {
		this.dateOfPickup = dateOfPickup;
	}

	public Date getDateOfNextVisit() {
		return dateOfNextVisit;
	}

	public void setDateOfNextVisit(Date dateOfNextVisit) {
		this.dateOfNextVisit = dateOfNextVisit;
	}

	public String getAccompagnateurName() {
		return accompagnateurName;
	}

	public void setAccompagnateurName(String accompagnateurName) {
		this.accompagnateurName = accompagnateurName;
	}

	public Boolean getPickedUpCtx() {
		return pickedUpCtx;
	}

	public void setPickedUpCtx(Boolean pickedUpCtx) {
		this.pickedUpCtx = pickedUpCtx;
	}

	public Boolean getPickedUpInh() {
		return pickedUpInh;
	}

	public void setPickedUpInh(Boolean pickedUpInh) {
		this.pickedUpInh = pickedUpInh;
	}

	public String getReasonForNotComing() {
		return reasonForNotComing;
	}

	public void setReasonForNotComing(String reasonForNotComing) {
		this.reasonForNotComing = reasonForNotComing;
	}
}
