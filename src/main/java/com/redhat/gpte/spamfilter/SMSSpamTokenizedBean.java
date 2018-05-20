package com.redhat.gpte.spamfilter;

import java.io.Serializable;

public class SMSSpamTokenizedBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6363306191691896038L;
	private Double labelDouble;
	private String tokens;
	
	public SMSSpamTokenizedBean(Double labelDouble, String tokens) {
		super();
		this.labelDouble = labelDouble;
		this.tokens = tokens;
	}
	public Double getLabelDouble() {
		return labelDouble;
	}
	public void setLabelDouble(Double labelDouble) {
		this.labelDouble = labelDouble;
	}
	public String getTokens() {
		return tokens;
	}
	public void setTokens(String tokens) {
		this.tokens = tokens;
	}
}
