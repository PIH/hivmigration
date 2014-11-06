package org.pih.hivmigration.common;

public class FollowupEncounter extends ClinicalEncounter {

	private String progress;
	private Boolean wellFollowed;
	private Boolean medToxicity;
	private String medToxicityComments;

	public FollowupEncounter() {}

	public String getProgress() {
		return progress;
	}

	public void setProgress(String progress) {
		this.progress = progress;
	}

	public Boolean getWellFollowed() {
		return wellFollowed;
	}

	public void setWellFollowed(Boolean wellFollowed) {
		this.wellFollowed = wellFollowed;
	}

	public Boolean getMedToxicity() {
		return medToxicity;
	}

	public void setMedToxicity(Boolean medToxicity) {
		this.medToxicity = medToxicity;
	}

	public String getMedToxicityComments() {
		return medToxicityComments;
	}

	public void setMedToxicityComments(String medToxicityComments) {
		this.medToxicityComments = medToxicityComments;
	}
}
