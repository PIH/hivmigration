package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.ActivityFrequency;
import org.pih.hivmigration.common.code.ArvTreatmentNeeded;
import org.pih.hivmigration.common.code.ExposureType;
import org.pih.hivmigration.common.code.Location;
import org.pih.hivmigration.common.code.PartnerReferralStatus;
import org.pih.hivmigration.common.code.TbTreatmentNeeded;
import org.pih.hivmigration.common.code.TreatmentStatus;
import org.pih.hivmigration.common.code.WhoStage;
import org.pih.hivmigration.common.util.ObsName;

import java.util.Date;
import java.util.List;

/**
 * Represents the commonality between an intake and a followup encounter.
 */
public class ClinicalEncounter extends Encounter {

	private Location location;
	private String examiningDoctor;
	private String recommendations;
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
	private Double weight;
	private Double height;
	private Double bmi;
	private Double systolicBloodPressure;
	private Double diastolicBloodPressure;
	private Double heartRate;
	private Double respirationRate;
	private Double temperature;

	@ObsName("arv_treatment_needed") ArvTreatmentNeeded arvTreatmentNeeded;
	@ObsName("arvs_for_accident") ExposureType arvsForAccident;
	@ObsName("arvs_for_ptme") Boolean arvsForPtme;
	@ObsName("adherence_accompagnateur") Boolean accompagnateurSeenEachDay; // TODO: Why not just followup
	@ObsName("adherence_missed_last_four_days") Integer adherenceMissedLastFourDays; // TODO: Why not just followup
	@ObsName("hospitalized_since_last_visit") Boolean hospitalizedSinceLastVisit; // TODO: Why not just followup
	@ObsName("rdv_birth_place") String birthplace;
	@ObsName("regimen_changed_p") Boolean regimenChanged;
	@ObsName("side_effects_present") Boolean sideEffectsPresent;
	@ObsName("tb_treatment_needed") TbTreatmentNeeded tbTreatmentNeeded;
	@ObsName("treatment_status") TreatmentStatus treatmentStatus;
	@ObsName("treatment_status_date_died") Date dateDied;
	@ObsName("treatment_status_date_abandoned") Date dateAbandoned;
	@ObsName("treatment_status_stopped_comment") String treatmentStoppedComment;
	@ObsName("treatment_status_transferred_comment") String transferredComment;

	public ClinicalEncounter() {}

	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}

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

	public Double getWeight() {
		return weight;
	}

	public void setWeight(Double weight) {
		this.weight = weight;
	}

	public Double getHeight() {
		return height;
	}

	public void setHeight(Double height) {
		this.height = height;
	}

	public Double getBmi() {
		return bmi;
	}

	public void setBmi(Double bmi) {
		this.bmi = bmi;
	}

	public Double getSystolicBloodPressure() {
		return systolicBloodPressure;
	}

	public void setSystolicBloodPressure(Double systolicBloodPressure) {
		this.systolicBloodPressure = systolicBloodPressure;
	}

	public Double getDiastolicBloodPressure() {
		return diastolicBloodPressure;
	}

	public void setDiastolicBloodPressure(Double diastolicBloodPressure) {
		this.diastolicBloodPressure = diastolicBloodPressure;
	}

	public Double getHeartRate() {
		return heartRate;
	}

	public void setHeartRate(Double heartRate) {
		this.heartRate = heartRate;
	}

	public Double getRespirationRate() {
		return respirationRate;
	}

	public void setRespirationRate(Double respirationRate) {
		this.respirationRate = respirationRate;
	}

	public Double getTemperature() {
		return temperature;
	}

	public void setTemperature(Double temperature) {
		this.temperature = temperature;
	}

	public ArvTreatmentNeeded getArvTreatmentNeeded() {
		return arvTreatmentNeeded;
	}

	public void setArvTreatmentNeeded(ArvTreatmentNeeded arvTreatmentNeeded) {
		this.arvTreatmentNeeded = arvTreatmentNeeded;
	}

	public ExposureType getArvsForAccident() {
		return arvsForAccident;
	}

	public void setArvsForAccident(ExposureType arvsForAccident) {
		this.arvsForAccident = arvsForAccident;
	}

	public Boolean getArvsForPtme() {
		return arvsForPtme;
	}

	public void setArvsForPtme(Boolean arvsForPtme) {
		this.arvsForPtme = arvsForPtme;
	}

	public Boolean getAccompagnateurSeenEachDay() {
		return accompagnateurSeenEachDay;
	}

	public void setAccompagnateurSeenEachDay(Boolean accompagnateurSeenEachDay) {
		this.accompagnateurSeenEachDay = accompagnateurSeenEachDay;
	}

	public Integer getAdherenceMissedLastFourDays() {
		return adherenceMissedLastFourDays;
	}

	public void setAdherenceMissedLastFourDays(Integer adherenceMissedLastFourDays) {
		this.adherenceMissedLastFourDays = adherenceMissedLastFourDays;
	}

	public Boolean getHospitalizedSinceLastVisit() {
		return hospitalizedSinceLastVisit;
	}

	public void setHospitalizedSinceLastVisit(Boolean hospitalizedSinceLastVisit) {
		this.hospitalizedSinceLastVisit = hospitalizedSinceLastVisit;
	}

	public String getBirthplace() {
		return birthplace;
	}

	public void setBirthplace(String birthplace) {
		this.birthplace = birthplace;
	}

	public Boolean getRegimenChanged() {
		return regimenChanged;
	}

	public void setRegimenChanged(Boolean regimenChanged) {
		this.regimenChanged = regimenChanged;
	}

	public Boolean getSideEffectsPresent() {
		return sideEffectsPresent;
	}

	public void setSideEffectsPresent(Boolean sideEffectsPresent) {
		this.sideEffectsPresent = sideEffectsPresent;
	}

	public TbTreatmentNeeded getTbTreatmentNeeded() {
		return tbTreatmentNeeded;
	}

	public void setTbTreatmentNeeded(TbTreatmentNeeded tbTreatmentNeeded) {
		this.tbTreatmentNeeded = tbTreatmentNeeded;
	}

	public TreatmentStatus getTreatmentStatus() {
		return treatmentStatus;
	}

	public void setTreatmentStatus(TreatmentStatus treatmentStatus) {
		this.treatmentStatus = treatmentStatus;
	}

	public Date getDateDied() {
		return dateDied;
	}

	public void setDateDied(Date dateDied) {
		this.dateDied = dateDied;
	}

	public Date getDateAbandoned() {
		return dateAbandoned;
	}

	public void setDateAbandoned(Date dateAbandoned) {
		this.dateAbandoned = dateAbandoned;
	}

	public String getTreatmentStoppedComment() {
		return treatmentStoppedComment;
	}

	public void setTreatmentStoppedComment(String treatmentStoppedComment) {
		this.treatmentStoppedComment = treatmentStoppedComment;
	}

	public String getTransferredComment() {
		return transferredComment;
	}

	public void setTransferredComment(String transferredComment) {
		this.transferredComment = transferredComment;
	}
}
