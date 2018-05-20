package com.redhat.gpte.creditrisk;

public class Credit {
	private double creditability;
	private double balance;
	private double duration;
	private double history;
	private double purpose;
	private double amount;
	private double savings;
	private double employment;
	private double instPercent;
	private double sexMarried;
	private double guarantors;
	private double residenceDuration;
	private double assets;
	private double age;
	private double concCredit;
	private double apartment;
	private double credits;
	private double occupation;
	private double dependents;
	private double hasPhone;
	private double foreign;

	public Credit(double creditability, double balance, double duration, double history, double purpose, double amount,
			double savings, double employment, double instPercent, double sexMarried, double guarantors,
			double residenceDuration, double assets, double age, double concCredit, double apartment, double credits,
			double occupation, double dependents, double hasPhone, double foreign) {
		super();
		this.creditability = creditability;
		this.balance = balance;
		this.duration = duration;
		this.history = history;
		this.purpose = purpose;
		this.amount = amount;
		this.savings = savings;
		this.employment = employment;
		this.instPercent = instPercent;
		this.sexMarried = sexMarried;
		this.guarantors = guarantors;
		this.residenceDuration = residenceDuration;
		this.assets = assets;
		this.age = age;
		this.concCredit = concCredit;
		this.apartment = apartment;
		this.credits = credits;
		this.occupation = occupation;
		this.dependents = dependents;
		this.hasPhone = hasPhone;
		this.foreign = foreign;
	}

	public double getCreditability() {
		return creditability;
	}

	public void setCreditability(double creditability) {
		this.creditability = creditability;
	}

	public double getBalance() {
		return balance;
	}

	public void setBalance(double balance) {
		this.balance = balance;
	}

	public double getDuration() {
		return duration;
	}

	public void setDuration(double duration) {
		this.duration = duration;
	}

	public double getHistory() {
		return history;
	}

	public void setHistory(double history) {
		this.history = history;
	}

	public double getPurpose() {
		return purpose;
	}

	public void setPurpose(double purpose) {
		this.purpose = purpose;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	public double getSavings() {
		return savings;
	}

	public void setSavings(double savings) {
		this.savings = savings;
	}

	public double getEmployment() {
		return employment;
	}

	public void setEmployment(double employment) {
		this.employment = employment;
	}

	public double getInstPercent() {
		return instPercent;
	}

	public void setInstPercent(double instPercent) {
		this.instPercent = instPercent;
	}

	public double getSexMarried() {
		return sexMarried;
	}

	public void setSexMarried(double sexMarried) {
		this.sexMarried = sexMarried;
	}

	public double getGuarantors() {
		return guarantors;
	}

	public void setGuarantors(double guarantors) {
		this.guarantors = guarantors;
	}

	public double getResidenceDuration() {
		return residenceDuration;
	}

	public void setResidenceDuration(double residenceDuration) {
		this.residenceDuration = residenceDuration;
	}

	public double getAssets() {
		return assets;
	}

	public void setAssets(double assets) {
		this.assets = assets;
	}

	public double getAge() {
		return age;
	}

	public void setAge(double age) {
		this.age = age;
	}

	public double getConcCredit() {
		return concCredit;
	}

	public void setConcCredit(double concCredit) {
		this.concCredit = concCredit;
	}

	public double getApartment() {
		return apartment;
	}

	public void setApartment(double apartment) {
		this.apartment = apartment;
	}

	public double getCredits() {
		return credits;
	}

	public void setCredits(double credits) {
		this.credits = credits;
	}

	public double getOccupation() {
		return occupation;
	}

	public void setOccupation(double occupation) {
		this.occupation = occupation;
	}

	public double getDependents() {
		return dependents;
	}

	public void setDependents(double dependents) {
		this.dependents = dependents;
	}

	public double getHasPhone() {
		return hasPhone;
	}

	public void setHasPhone(double hasPhone) {
		this.hasPhone = hasPhone;
	}

	public double getForeign() {
		return foreign;
	}

	public void setForeign(double foreign) {
		this.foreign = foreign;
	}
}
