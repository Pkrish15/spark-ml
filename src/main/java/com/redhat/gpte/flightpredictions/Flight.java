package com.redhat.gpte.flightpredictions;

import java.io.Serializable;

public class Flight implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1906978312043055649L;
	double label;
	double monthDay;
	double weekDay;
	double crsdeptime;
	double crsarrtime;
	String carrier;
	double crselapsedtime;
	double ArrDelayMinutes;
	double distance;
	String origin;
	String dest;

	public Flight(double label, double monthDay, double weekDay, double crsdeptime, double ArrDelayMinutes, double distance,double crsarrtime, String carrier,
			double crselapsedtime, String origin, String dest) {
		super();
		this.label = label;
		this.monthDay = monthDay;
		this.weekDay = weekDay;
		this.crsdeptime = crsdeptime;
		this.crsarrtime = crsarrtime;
		this.carrier = carrier;
		this.crselapsedtime = crselapsedtime;
		this.ArrDelayMinutes=ArrDelayMinutes;
		this.distance=distance;
		this.origin = origin;
		this.dest = dest;
	}

	public double getLabel() {
		return label;
	}

	public void setLabel(double label) {
		this.label = label;
	}

	public double getMonthDay() {
		return monthDay;
	}

	public void setMonthDay(double monthDay) {
		this.monthDay = monthDay;
	}

	public double getWeekDay() {
		return weekDay;
	}

	public void setWeekDay(double weekDay) {
		this.weekDay = weekDay;
	}

	public double getCrsdeptime() {
		return crsdeptime;
	}

	public void setCrsdeptime(double crsdeptime) {
		this.crsdeptime = crsdeptime;
	}

	public double getCrsarrtime() {
		return crsarrtime;
	}

	public void setCrsarrtime(double crsarrtime) {
		this.crsarrtime = crsarrtime;
	}

	public String getCarrier() {
		return carrier;
	}

	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}

	public double getCrselapsedtime() {
		return crselapsedtime;
	}

	public void setCrselapsedtime(double crselapsedtime) {
		this.crselapsedtime = crselapsedtime;
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
	}

	@Override
	public String toString() {
		return "Flight [label=" + label + ", monthDay=" + monthDay + ", weekDay=" + weekDay + ", crsdeptime="
				+ crsdeptime + ", crsarrtime=" + crsarrtime + ", carrier=" + carrier + ", crselapsedtime="
				+ crselapsedtime + ", origin=" + origin + ", dest=" + dest + "]";
	}

	public double getArrdelayminutes() {
		return ArrDelayMinutes;
	}

	public void setArrdelayminutes(double ArrDelayMinutes) {
		this.ArrDelayMinutes = ArrDelayMinutes;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}
}