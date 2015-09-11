package cn.edu.pku.zx.ali.predict.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Initialize a user-behavior object from a string in which each property is
 * separated by a comma;
 * 
 * @author zhangxu
 * 
 */
public class UserBehaviorWritable implements
		WritableComparable<UserBehaviorWritable> {
	private static final String FIELDS_SEPARATOR = ",";
	private static final String DATE_HOUR_SEPARATOR = " ";
	private static final String YMD_SEPARATOR = "-";

	private static final int USER_ID_LOC = 0;
	private static final int ITEM_ID_LOC = 1;
	private static final int BEHAVIOR_TYPE_LOC = 2;
	private static final int ITEM_CATEGORY_LOC = 4;
	private static final int TIME_LOC = 5;

	private static final int DATE_LOC = 0;
	private static final int HOUR_LOC = 1;
	private static final int YEAR_LOC = 0;
	private static final int MONTH_LOC = 1;
	private static final int DAY_LOC = 2;

	public static final int BEHAVIOR_TYPE_BROWSE=1;
	public static final int BEHAVIOR_TYPE_FAVORITE=2;
	public static final int BEHAVIOR_TYPE_CART=3;
	public static final int BEHAVIOR_TYPE_BUY=4;
	
	private LongWritable user_id;
	private LongWritable item_id;
	private IntWritable behavior_type;// 1 means browse, 2 means favorite, 3
										// means carted, 4 means buy;
	private LongWritable item_category;
	private DateTimeWritable time;

	/**
	 * map-reduce framework serialization will initialize a writable object with
	 * zero parameter constructor
	 */
	public UserBehaviorWritable() {
		user_id = new LongWritable();
		item_id = new LongWritable();
		behavior_type = new IntWritable();
		item_category = new LongWritable();
		time = new DateTimeWritable();
	}

	public static UserBehaviorWritable getInstance(String userInfo) {
		UserBehaviorWritable ub = new UserBehaviorWritable();

		String[] fields = userInfo.split(FIELDS_SEPARATOR);
		ub.user_id = new LongWritable(Long.parseLong(fields[USER_ID_LOC]));
		ub.item_id = new LongWritable(Long.parseLong(fields[ITEM_ID_LOC]));
		ub.behavior_type = new IntWritable(Integer.parseInt(fields[BEHAVIOR_TYPE_LOC]));
		ub.item_category = new LongWritable(Long.parseLong(fields[ITEM_CATEGORY_LOC]));

		String[] timeParts = fields[TIME_LOC].split(DATE_HOUR_SEPARATOR);
		int hour = Integer.parseInt(timeParts[HOUR_LOC]);
		String[] ymd = timeParts[DATE_LOC].split(YMD_SEPARATOR);
		int year = Integer.parseInt(ymd[YEAR_LOC]);
		int month = Integer.parseInt(ymd[MONTH_LOC]);
		int day = Integer.parseInt(ymd[DAY_LOC]);
		ub.time = new DateTimeWritable(year, month, day, hour);

		return ub;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		user_id.write(out);
		item_id.write(out);
		behavior_type.write(out);
		item_category.write(out);
		time.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		user_id.readFields(in);
		item_id.readFields(in);
		behavior_type.readFields(in);
		item_category.readFields(in);
		time.readFields(in);
	}

	
	
	/**
	 * comparison order: user_id, time, item_category, item_id 
	 */
	@Override
	public int compareTo(UserBehaviorWritable ub) {
		int ret = user_id.compareTo(ub.user_id);
		if (0 != ret)
			return ret;
		
		ret = time.compareTo(ub.time);
		if (0 != ret)
			return ret;
		
		ret = item_category.compareTo(ub.item_category);
		if (0 != ret)
			return ret;
			
		return item_id.compareTo(ub.item_id);
	}

	@Override
	public boolean equals(Object o) {
		if (null == o)
			return false;
		if (this == o)
			return true;

		if (!(o instanceof UserBehaviorWritable))
			return false;
		
		UserBehaviorWritable that = (UserBehaviorWritable) o;
		return (user_id.equals(that.user_id)&& item_id.equals(that.item_id)
				&& behavior_type.equals(that.behavior_type)
				&& item_category.equals(that.item_category)
				&& time.equals(that.time));
	}

	@Override
	public int hashCode() {
		int result = user_id.hashCode();
		result = 31 * result + item_id.hashCode();
		result = 31 * result + behavior_type.hashCode();
		result = 31 * result + item_category.hashCode();
		result = 31 * result + time.hashCode();
		return result;
	}

	
	
	@Override
	public Object clone()  {
		UserBehaviorWritable ub=new UserBehaviorWritable();
		ub.setUser_id(new LongWritable(getUser_id().get()));
		ub.setItem_id(new LongWritable(getItem_id().get()));
		ub.setBehavior_type(new IntWritable(getBehavior_type().get()));
		ub.setItem_category(new LongWritable(getItem_category().get()));
		ub.setTime((DateTimeWritable) getTime().clone());
		
		return ub;
	}

	@Override
	public String toString() {
		return "" + user_id + "\t" + time+ "\t"+item_category+"\t"+item_id;
	}

	public LongWritable getUser_id() {
		return user_id;
	}

	public void setUser_id(LongWritable user_id) {
		this.user_id = user_id;
	}

	public LongWritable getItem_id() {
		return item_id;
	}

	public void setItem_id(LongWritable item_id) {
		this.item_id = item_id;
	}

	public IntWritable getBehavior_type() {
		return behavior_type;
	}

	public void setBehavior_type(IntWritable behavior_type) {
		this.behavior_type = behavior_type;
	}

	public LongWritable getItem_category() {
		return item_category;
	}

	public void setItem_category(LongWritable item_category) {
		this.item_category = item_category;
	}

	public DateTimeWritable getTime() {
		return time;
	}

	public void setTime(DateTimeWritable time) {
		this.time = time;
	}

	/**
	 * implement a specific comparator to boost performance in comparing two
	 * binary DateTimeWritable objects
	 */
	public static class Comparator extends WritableComparator {
		private static final LongWritable.Comparator LONG_COMPARATOR = new LongWritable.Comparator();
		private static final DateTimeWritable.Comparator DATETIME_COMPARATOR = new DateTimeWritable.Comparator();
		private static final int INT_SIZE = Integer.SIZE / 8;
		private static final int LONG_SIZE = Long.SIZE / 8;

		public Comparator() {
			super(UserBehaviorWritable.class);
		}

		/**
		 * comparison order: user_id, time, item_category, item_id 
		 */
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			//first comparing:  long bytes:user_id
			int cmp = LONG_COMPARATOR.compare(b1, s1, LONG_SIZE, b2, s2,
					LONG_SIZE);
			if (cmp != 0)
				return cmp;

			// second comparing:  DateTimeWritable bytes: time
			cmp = DATETIME_COMPARATOR.compare(b1, s1 + LONG_SIZE * 3
					+ INT_SIZE, l1-(LONG_SIZE * 3+ INT_SIZE), b2, s2 + LONG_SIZE * 3 + INT_SIZE,
					l2-(LONG_SIZE * 3+ INT_SIZE));			
			if (cmp != 0)
				return cmp;

			// third comparing:  long bytes: item_category
			cmp =LONG_COMPARATOR.compare(b1, s1 + INT_SIZE + LONG_SIZE * 2,
					LONG_SIZE, b2, s2 + INT_SIZE + LONG_SIZE * 2, LONG_SIZE); 
			if (cmp != 0)
				return cmp;

			// fourth comparing: long bytes: item_id
			return LONG_COMPARATOR.compare(b1, s1 + LONG_SIZE, LONG_SIZE, b2, s2
					+ LONG_SIZE, LONG_SIZE);
		}
	}

	/**
	 * register this comparator so that hadoop could find it when it is needed
	 * in comparing two binary Date_md objects
	 */
	static {
		WritableComparator.define(UserBehaviorWritable.class, new Comparator());
	}
}
