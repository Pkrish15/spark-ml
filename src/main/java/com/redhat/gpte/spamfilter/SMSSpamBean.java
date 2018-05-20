package com.redhat.gpte.spamfilter;

import java.io.Serializable;

public class SMSSpamBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5643352064512069426L;
	private String labelString;
	private String featureString;
	
	public SMSSpamBean(String labelString, String featureString) {
		super();
		this.labelString = labelString;
		this.featureString = featureString;
	}
	public String getLabelString() {
		return labelString;
	}
	public void setLabelString(String labelString) {
		this.labelString = labelString;
	}
	public String getFeatureString() {
		return featureString;
	}
	public void setFeatureString(String featureString) {
		this.featureString = featureString;
	}
}
