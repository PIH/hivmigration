package org.pih.hivmigration.common;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Pregnancy {

	private Integer pregnancyId;
	private Date lastPeriodDate;
	private Date expectedDeliveryDate;
	private Integer gravidity;
	private Integer parity;
	private Integer numLivingChildren;
	private Integer numAbortions;
	private String familyPlanningMethod;
	private String postOutcomeFamilyPlanning;
	private String comments;
	private String outcome;
	private Date outcomeDate;
	private String outcomeLocation;
	private String outcomeMethod;

	private List<PostnatalEncounter> postnatalEncounters;

	public Pregnancy() {}

	public Integer getPregnancyId() {
		return pregnancyId;
	}

	public void setPregnancyId(Integer pregnancyId) {
		this.pregnancyId = pregnancyId;
	}

	public Date getLastPeriodDate() {
		return lastPeriodDate;
	}

	public void setLastPeriodDate(Date lastPeriodDate) {
		this.lastPeriodDate = lastPeriodDate;
	}

	public Date getExpectedDeliveryDate() {
		return expectedDeliveryDate;
	}

	public void setExpectedDeliveryDate(Date expectedDeliveryDate) {
		this.expectedDeliveryDate = expectedDeliveryDate;
	}

	public Integer getGravidity() {
		return gravidity;
	}

	public void setGravidity(Integer gravidity) {
		this.gravidity = gravidity;
	}

	public Integer getParity() {
		return parity;
	}

	public void setParity(Integer parity) {
		this.parity = parity;
	}

	public Integer getNumLivingChildren() {
		return numLivingChildren;
	}

	public void setNumLivingChildren(Integer numLivingChildren) {
		this.numLivingChildren = numLivingChildren;
	}

	public Integer getNumAbortions() {
		return numAbortions;
	}

	public void setNumAbortions(Integer numAbortions) {
		this.numAbortions = numAbortions;
	}

	public String getFamilyPlanningMethod() {
		return familyPlanningMethod;
	}

	public void setFamilyPlanningMethod(String familyPlanningMethod) {
		this.familyPlanningMethod = familyPlanningMethod;
	}

	public String getPostOutcomeFamilyPlanning() {
		return postOutcomeFamilyPlanning;
	}

	public void setPostOutcomeFamilyPlanning(String postOutcomeFamilyPlanning) {
		this.postOutcomeFamilyPlanning = postOutcomeFamilyPlanning;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}

	public String getOutcome() {
		return outcome;
	}

	public void setOutcome(String outcome) {
		this.outcome = outcome;
	}

	public Date getOutcomeDate() {
		return outcomeDate;
	}

	public void setOutcomeDate(Date outcomeDate) {
		this.outcomeDate = outcomeDate;
	}

	public String getOutcomeLocation() {
		return outcomeLocation;
	}

	public void setOutcomeLocation(String outcomeLocation) {
		this.outcomeLocation = outcomeLocation;
	}

	public String getOutcomeMethod() {
		return outcomeMethod;
	}

	public void setOutcomeMethod(String outcomeMethod) {
		this.outcomeMethod = outcomeMethod;
	}

	public List<PostnatalEncounter> getPostnatalEncounters() {
		if (postnatalEncounters == null) {
			postnatalEncounters = new ArrayList<PostnatalEncounter>();
		}
		return postnatalEncounters;
	}

	public void setPostnatalEncounters(List<PostnatalEncounter> postnatalEncounters) {
		this.postnatalEncounters = postnatalEncounters;
	}

	public void addPostnatalEncounter(PostnatalEncounter encounter) {
		getPostnatalEncounters().add(encounter);
	}
}
