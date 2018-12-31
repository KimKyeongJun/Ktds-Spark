package com.ktds.log.statistics;

import java.io.Serializable;

public class Statistics implements Serializable {

	private static final long serialVersionUID = 1064519603057212447L;

	private final String ip;

	private final String url;

	private final String hour;
	private final String minite;
	private final String second;

	private final int reqCount;
	
	public Statistics(String hour, String minite, String second, String url, String ip, int reqCount) {
		this.hour = hour;
		this.minite = minite;
		this.second = second;
		this.url = url;
		this.ip = ip;
		this.reqCount = reqCount;
	}
	
	public Statistics(String hour, String minite, String url, String ip, int reqCount) {
		this.hour = hour;
		this.minite = minite;
		this.second = "00";
		this.url = url;
		this.ip = ip;
		this.reqCount = reqCount;
	}
	
	public Statistics(String hour, String url, String ip, int reqCount) {
		this.hour = hour;
		this.minite = "00";
		this.second = "00";
		this.url = url;
		this.ip = ip;
		this.reqCount = reqCount;
	}

	public Statistics(String arr[]) {
		this.ip = arr[4];
		this.url = arr[3];
		this.hour = arr[0];
		this.minite = arr[1];
		this.second = arr[2];
		this.reqCount = 0;
	}

	public String getIp() {
		return ip;
	}

	public String getUrl() {
		return url;
	}

	public String getHour() {
		return hour;
	}

	public String getMinite() {
		return minite;
	}

	public String getSecond() {
		return second;
	}

	public int getReqCount() {
		return reqCount;
	}

}
