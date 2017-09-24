package com.oren.eventproces.mapreduce;

public class FrequectDistributionRef {

	private Double ua;
	private Double imp;
	private Long total;
	private Double threshold;
	public Double getUa() {
		return ua;
	}
	public void setUa(Double ua) {
		this.ua = ua;
	}
	public Double getImp() {
		return imp;
	}
	public void setImp(Double imp) {
		this.imp = imp;
	}
	public Long getTotal() {
		return total;
	}
	public void setTotal(Long total) {
		this.total = total;
	}
	public Double getThreshold() {
		return threshold;
	}
	public void setThreshold(Double threshold) {
		this.threshold = threshold;
	}
	@Override
	public String toString() {
		return "FrequectDistributionRef [ua=" + ua + ", imp=" + imp
				+ ", total=" + total + ", threshold=" + threshold + "]";
	}
	
	
}