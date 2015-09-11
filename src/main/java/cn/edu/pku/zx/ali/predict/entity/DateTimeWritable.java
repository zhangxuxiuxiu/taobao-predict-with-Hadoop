package cn.edu.pku.zx.ali.predict.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * all dates are between 11-18 and 12-18;
 * 
 * @author zhangxu
 * 
 */
public class DateTimeWritable implements WritableComparable<DateTimeWritable> {
	private IntWritable year;
	private IntWritable month;
	private IntWritable day;
	private IntWritable hour;

	public static final DateTimeWritable PredictionDate=new DateTimeWritable(2014,12,18,0);
	
	public DateTimeWritable() {
		this(0, 0, 0, 0);
	}

	public DateTimeWritable(int year, int month, int day, int hour) {
		super();
		this.year = new IntWritable(year);
		this.month = new IntWritable(month);
		this.day = new IntWritable(day);
		this.hour = new IntWritable(hour);
	}

	/** 
	 * @return interval between two dates.
	 */
	public int IntervalBetween(DateTimeWritable dt) {
		int days_1 = (12 - getMonth().get()) * 30 + getDay().get(), 
				days_2 = (12 - dt.getMonth().get()) * 30 + dt.getDay().get();

		return Math.abs(days_1 - days_2);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		year.write(out);
		month.write(out);
		day.write(out);
		hour.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year.readFields(in);
		month.readFields(in);
		day.readFields(in);
		hour.readFields(in);
	}

	@Override
	public int compareTo(DateTimeWritable dtw) {
		int ret = year.compareTo(dtw.year);
		if (ret != 0)
			return ret;
		
		ret = month.compareTo(dtw.month);
		if (ret != 0)
			return ret;
		
		ret = day.compareTo(dtw.day);
		if (ret != 0)
			return ret;
		
		return hour.compareTo(dtw.hour);
	}

	@Override
	public boolean equals(Object o) {
		if (null == o)
			return false;
		if (this == o)
			return true;

		if (!(o instanceof DateTimeWritable))
			return false;
		
		DateTimeWritable that = (DateTimeWritable) o;
		return compareTo(that)==0;
	}

	@Override
	public int hashCode() {
		int result = year.hashCode();
		result = 31 * result + month.hashCode();
		result = 31 * result + day.hashCode();
		result = 31 * result + hour.hashCode();
		return result;
	}

	@Override
	public String toString()
	{
		return ""+year+"-"+month+"-"+day+"-"+hour;
	}
	
	
	@Override
	public Object clone() {
		DateTimeWritable dt=new DateTimeWritable();
		dt.setHour(new IntWritable(getHour().get()));
		dt.setDay(new IntWritable(getDay().get()));
		dt.setMonth(new IntWritable(getMonth().get()));
		dt.setYear(new IntWritable(getYear().get()));
		return dt;
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

	public IntWritable getMonth() {
		return month;
	}

	public void setMonth(IntWritable month) {
		this.month = month;
	}

	public IntWritable getDay() {
		return day;
	}

	public void setDay(IntWritable day) {
		this.day = day;
	}

	public IntWritable getHour() {
		return hour;
	}

	public void setHour(IntWritable hour) {
		this.hour = hour;
	}

	
	/**
	 * implement a specific comparator to boost performance in comparing two
	 * binary DateTimeWritable objects
	 */
	public static class Comparator extends WritableComparator {
		private static final IntWritable.Comparator INT_COMPARATOR = new IntWritable.Comparator();
		private static final int INT_SIZE = Integer.SIZE / 8;

		public Comparator() {
			super(DateTimeWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// comparing first int bytes: year
			int cmp = INT_COMPARATOR
					.compare(b1, s1, INT_SIZE, b2, s2, INT_SIZE);
			if (cmp != 0)
				return cmp;

			// comparing second int bytes:month
			cmp = INT_COMPARATOR.compare(b1, s1 + INT_SIZE, INT_SIZE, b2, s2
					+ INT_SIZE, INT_SIZE);
			if (cmp != 0)
				return cmp;

			// comparing third int bytes:day
			cmp = INT_COMPARATOR.compare(b1, s1 + INT_SIZE * 2, INT_SIZE, b2,
					s2 + INT_SIZE * 2, INT_SIZE);
			if (cmp != 0)
				return cmp;

			// comparing the four int bytes:hour
			return INT_COMPARATOR.compare(b1, s1 + INT_SIZE * 3, INT_SIZE, b2,
					s2 + INT_SIZE * 3, INT_SIZE);
		}
	}

	/**
	 * register this comparator so that hadoop could find it when it is needed
	 * in comparing two binary Date_md objects
	 */
	static {
		WritableComparator.define(DateTimeWritable.class, new Comparator());
	}
}
