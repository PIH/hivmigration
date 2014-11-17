package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.ActivityFrequency;
import org.pih.hivmigration.common.code.PartnerReferralStatus;
import org.pih.hivmigration.common.code.WhoStage;

import java.util.Date;
import java.util.List;

/**
 * Represents the commonality between an intake and a followup encounter.
 */
public class ClinicalEncounter extends Encounter {

	private String examiningDoctor;
	private String recommendations;
	private String comments;
	private Boolean startFinancialAid;
	private Boolean continueFinancialAid;
	private String formVersion;
	private ResponsiblePerson responsiblePerson;
	private String presentingComplaint;
	private String physicalExamComments;
	private Boolean pregnant;
	private Date lastPeriodDate;
	private Date expectedDeliveryDate;
	private String mothersFirstName;
	private String mothersLastName;
	private Boolean postTestCounseling;
	private PartnerReferralStatus partnerReferralStatus;
	private Date nextExamDate;
	private WhoStage whoStage;
	private String mainActivityBefore;
	private ActivityFrequency mainActivityHowNow;
	private String otherActivitiesBefore;
	private ActivityFrequency otherActivitiesHowNow;
	private Boolean oiNow;
	private String planExtra;
	private List<OpportunisticInfection> opportunisticInfections;
	private List<SymptomGroup> symptomGroups;
	private List<LabTestOrder> labTestOrders;
	private List<GenericOrder> genericOrders;
	private List<LabTestResult> labResults;

	public ClinicalEncounter() {}

	public String getExaminingDoctor() {
		return examiningDoctor;
	}

	public void setExaminingDoctor(String examiningDoctor) {
		this.examiningDoctor = examiningDoctor;
	}

	public String getRecommendations() {
		return recommendations;
	}

	public void setRecommendations(String recommendations) {
		this.recommendations = recommendations;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}

	public Boolean getStartFinancialAid() {
		return startFinancialAid;
	}

	public void setStartFinancialAid(Boolean startFinancialAid) {
		this.startFinancialAid = startFinancialAid;
	}

	public Boolean getContinueFinancialAid() {
		return continueFinancialAid;
	}

	public void setContinueFinancialAid(Boolean continueFinancialAid) {
		this.continueFinancialAid = continueFinancialAid;
	}

	public String getFormVersion() {
		return formVersion;
	}

	public void setFormVersion(String formVersion) {
		this.formVersion = formVersion;
	}

	public ResponsiblePerson getResponsiblePerson() {
		return responsiblePerson;
	}

	public void setResponsiblePerson(ResponsiblePerson responsiblePerson) {
		this.responsiblePerson = responsiblePerson;
	}

	public String getPresentingComplaint() {
		return presentingComplaint;
	}

	public void setPresentingComplaint(String presentingComplaint) {
		this.presentingComplaint = presentingComplaint;
	}

	public String getPhysicalExamComments() {
		return physicalExamComments;
	}

	public void setPhysicalExamComments(String physicalExamComments) {
		this.physicalExamComments = physicalExamComments;
	}

	public Boolean getPregnant() {
		return pregnant;
	}

	public void setPregnant(Boolean pregnant) {
		this.pregnant = pregnant;
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

	public String getMothersFirstName() {
		return mothersFirstName;
	}

	public void setMothersFirstName(String mothersFirstName) {
		this.mothersFirstName = mothersFirstName;
	}

	public String getMothersLastName() {
		return mothersLastName;
	}

	public void setMothersLastName(String mothersLastName) {
		this.mothersLastName = mothersLastName;
	}

	public Boolean getPostTestCounseling() {
		return postTestCounseling;
	}

	public void setPostTestCounseling(Boolean postTestCounseling) {
		this.postTestCounseling = postTestCounseling;
	}

	public PartnerReferralStatus getPartnerReferralStatus() {
		return partnerReferralStatus;
	}

	public void setPartnerReferralStatus(PartnerReferralStatus partnerReferralStatus) {
		this.partnerReferralStatus = partnerReferralStatus;
	}

	public Date getNextExamDate() {
		return nextExamDate;
	}

	public void setNextExamDate(Date nextExamDate) {
		this.nextExamDate = nextExamDate;
	}

	public WhoStage getWhoStage() {
		return whoStage;
	}

	public void setWhoStage(WhoStage whoStage) {
		this.whoStage = whoStage;
	}

	public String getMainActivityBefore() {
		return mainActivityBefore;
	}

	public void setMainActivityBefore(String mainActivityBefore) {
		this.mainActivityBefore = mainActivityBefore;
	}

	public ActivityFrequency getMainActivityHowNow() {
		return mainActivityHowNow;
	}

	public void setMainActivityHowNow(ActivityFrequency mainActivityHowNow) {
		this.mainActivityHowNow = mainActivityHowNow;
	}

	public String getOtherActivitiesBefore() {
		return otherActivitiesBefore;
	}

	public void setOtherActivitiesBefore(String otherActivitiesBefore) {
		this.otherActivitiesBefore = otherActivitiesBefore;
	}

	public ActivityFrequency getOtherActivitiesHowNow() {
		return otherActivitiesHowNow;
	}

	public void setOtherActivitiesHowNow(ActivityFrequency otherActivitiesHowNow) {
		this.otherActivitiesHowNow = otherActivitiesHowNow;
	}

	public Boolean getOiNow() {
		return oiNow;
	}

	public void setOiNow(Boolean oiNow) {
		this.oiNow = oiNow;
	}

	public String getPlanExtra() {
		return planExtra;
	}

	public void setPlanExtra(String planExtra) {
		this.planExtra = planExtra;
	}

	public List<OpportunisticInfection> getOpportunisticInfections() {
		return opportunisticInfections;
	}

	public void setOpportunisticInfections(List<OpportunisticInfection> opportunisticInfections) {
		this.opportunisticInfections = opportunisticInfections;
	}

	public List<SymptomGroup> getSymptomGroups() {
		return symptomGroups;
	}

	public void setSymptomGroups(List<SymptomGroup> symptomGroups) {
		this.symptomGroups = symptomGroups;
	}

	public List<LabTestOrder> getLabTestOrders() {
		return labTestOrders;
	}

	public void setLabTestOrders(List<LabTestOrder> labTestOrders) {
		this.labTestOrders = labTestOrders;
	}

	public List<GenericOrder> getGenericOrders() {
		return genericOrders;
	}

	public void setGenericOrders(List<GenericOrder> genericOrders) {
		this.genericOrders = genericOrders;
	}

	public List<LabTestResult> getLabResults() {
		return labResults;
	}

	public void setLabResults(List<LabTestResult> labResults) {
		this.labResults = labResults;
	}
}
