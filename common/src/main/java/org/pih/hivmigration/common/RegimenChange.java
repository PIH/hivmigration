package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.ExposureType;
import org.pih.hivmigration.common.code.SideEffect;
import org.pih.hivmigration.common.util.ObsName;

import java.util.Date;
import java.util.List;

/**
 * Represents a record af an accompagnateur picking up medications for a patient
 */
public class RegimenChange extends Encounter {

	@ObsName("arvs_for_accident") private ExposureType arvsForExposureType;
	@ObsName("arvs_for_ptme") private Boolean arvsForPtme;
	@ObsName("arv_for_ptme_start_date") private Date arvsForPtmeStartDate;
	@ObsName("side_effect") private List<SideEffect> sideEffects;
	@ObsName("side_effect_other") private String sideEffectOther;

	public RegimenChange() {}

	public ExposureType getArvsForExposureType() {
		return arvsForExposureType;
	}

	public void setArvsForExposureType(ExposureType arvsForExposureType) {
		this.arvsForExposureType = arvsForExposureType;
	}

	public Boolean getArvsForPtme() {
		return arvsForPtme;
	}

	public void setArvsForPtme(Boolean arvsForPtme) {
		this.arvsForPtme = arvsForPtme;
	}

	public Date getArvsForPtmeStartDate() {
		return arvsForPtmeStartDate;
	}

	public void setArvsForPtmeStartDate(Date arvsForPtmeStartDate) {
		this.arvsForPtmeStartDate = arvsForPtmeStartDate;
	}

	public List<SideEffect> getSideEffects() {
		return sideEffects;
	}

	public void setSideEffects(List<SideEffect> sideEffects) {
		this.sideEffects = sideEffects;
	}

	public String getSideEffectOther() {
		return sideEffectOther;
	}

	public void setSideEffectOther(String sideEffectOther) {
		this.sideEffectOther = sideEffectOther;
	}
}
