package com.redhat.gpte.mllib.pipeline;

import java.io.Serializable;

public class SMSSpamHamLabelDocument implements Serializable {
	  /**
	 * 
	 */
	private static final long serialVersionUID = -4734875315496262950L;
	private double label;
	  private String wordText;

	  public SMSSpamHamLabelDocument(double label, String wordText) {
	    this.label = label;
	    this.wordText = wordText;
	  }

	  public double getLabel() { return this.label; }
	  public void setLabel(double id) { this.label = label; }

	  public String getWordText() { return this.wordText; }
	  public void setWordText(String wordText) { this.wordText = wordText; }
}	