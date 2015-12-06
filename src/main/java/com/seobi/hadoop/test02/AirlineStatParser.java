package com.seobi.hadoop.test02;

import org.apache.hadoop.io.Text;

public class AirlineStatParser {
	private int year;
	private int month;
	private boolean isArrivalDelay;
	private int arrivalDelay;
	private boolean isDepartureDelay;
	private int departureDelay;
	
	public AirlineStatParser(Text text) {
		try{
			//System.err.println("input Text : " + text);
			String[] value = text.toString().split(",");
			
			if( value.length < 20 )
				return;
			
			year = Integer.parseInt(value[0]);
			month = Integer.parseInt(value[1]);
			
			if( "NA".equals(value[14]) ) {
				isArrivalDelay = false;
			} else {
				isArrivalDelay = true;
				arrivalDelay = Integer.parseInt(value[14]);
			}
			
			if( "NA".equals(value[15]) ) {
				isDepartureDelay = false;
			} else {
				isDepartureDelay = true;
				departureDelay = Integer.parseInt(value[15]);
			}
			//System.err.println("getYearMonth() : " + getYearMonth());
		} catch (Exception e) {
			System.err.println("AirlineStatParser Exception" + e);
		}
	}
	
	public Text getYearMonth() {
		Text text = new Text();;
		text.set(year + "-" + month);
		return text;
	}
	
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public boolean isDepartureDelay() {
		return isDepartureDelay;
	}
	public void setDepartureDelay(boolean isDepartureDelay) {
		this.isDepartureDelay = isDepartureDelay;
	}
	public int getDepartureDelay() {
		return departureDelay;
	}
	public void setDepartureDelay(int departureDelay) {
		this.departureDelay = departureDelay;
	}
	public boolean isArrivalDelay() {
		return isArrivalDelay;
	}
	public void setArrivalDelay(boolean isArrivalDelay) {
		this.isArrivalDelay = isArrivalDelay;
	}
	public int getArrivalDelay() {
		return arrivalDelay;
	}
	public void setArrivalDelay(int arrivalDelay) {
		this.arrivalDelay = arrivalDelay;
	}
	
}
