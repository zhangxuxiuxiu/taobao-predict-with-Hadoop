package cn.edu.pku.zx.ali.predict.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class LongPairWritable implements WritableComparable<LongPairWritable> {
	private LongWritable user_id;
	private LongWritable item_id;
	
	public LongPairWritable() {
		user_id=new LongWritable();
		item_id=new LongWritable();
	}

	public LongPairWritable(LongWritable user_id, LongWritable item_id) {
		super();
		this.user_id = user_id;
		this.item_id = item_id;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		user_id.write(out);
		item_id.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		user_id.readFields(in);
		item_id.readFields(in);
	}

	@Override
	public int compareTo(LongPairWritable lp) {	
		int cmp=user_id.compareTo(lp.user_id);
		if(0!=cmp)
			return cmp;
		
		return item_id.compareTo(lp.item_id);
	}

	@Override
	public int hashCode() {		
		return user_id.hashCode()+163*item_id.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(null==obj) 
			return false;
		if(this==obj)
			return true;
		
		if(!(obj instanceof LongPairWritable))
			return false;
		
		LongPairWritable lp=(LongPairWritable)obj;
		return (user_id.equals(lp.user_id)&&item_id.equals(lp.item_id));
	}

	@Override
	public String toString() {		
		return ""+user_id+","+item_id;
	}

	public static class Comparator extends WritableComparator
	{
		private static final LongWritable.Comparator LONG_COMPARATOR=new LongWritable.Comparator();
		private static final int LONG_SIZE= Long.SIZE/8;
		
		public Comparator() {
			super(LongPairWritable.class,true);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int cmp=LONG_COMPARATOR.compare(b1, s1, LONG_SIZE, b2, s2, LONG_SIZE);
			if(0!=cmp)
				return cmp;
			
			return LONG_COMPARATOR.compare(b1, s1+LONG_SIZE, LONG_SIZE, b2, s2+LONG_SIZE, LONG_SIZE);
		}
	}
	
	static {
		WritableComparator.define(LongPairWritable.class, new Comparator());
	}
}
