package cn.edu.pku.zx.ali.predict.config;

public abstract class BaseConfigurer implements IConfigurable{
	private int max_inactive_days;
	private int min_times;
	private int ordinary_times;
	
	protected BaseConfigurer(int max_inactive_days, int min_times,
			int ordinary_times) {
		super();
		this.max_inactive_days = max_inactive_days;
		this.min_times = min_times;
		this.ordinary_times = ordinary_times;
	}
	
	
	public final int getMax_Inactive_Days() {		
		return max_inactive_days;
	}
	
	public final int getMin_Times() {
		return min_times;
	}

	public final int getOrdinary_Times() {
		return ordinary_times;
	}
}
