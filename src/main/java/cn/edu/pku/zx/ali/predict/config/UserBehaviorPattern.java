package cn.edu.pku.zx.ali.predict.config;

import java.util.LinkedList;
import java.util.List;

import cn.edu.pku.zx.ali.predict.entity.UserBehaviorWritable;

public class UserBehaviorPattern {
	//private List<BehaviorUnit> serials;
	
	public UserBehaviorPattern()
	{
		//serials=new LinkedList<BehaviorUnit>();
	}
	
	public static UserBehaviorPattern analyze(LinkedList<LinkedList<UserBehaviorWritable>> periods) {		
		UserBehaviorPattern ubp = new UserBehaviorPattern();
		for(LinkedList<UserBehaviorWritable> period: periods)	
			ubp.take(extractPattern(period));
		
		return ubp;
	}

	private UserBehaviorPattern take(UserBehaviorPattern ubp)
	{
		return ubp;
	}
	
	public Iterable<Long> match(LinkedList<UserBehaviorWritable> period)
	{
		List<Long> matches=new LinkedList<Long>();
		
		return matches;
	}
	
	private static UserBehaviorPattern extractPattern(LinkedList<UserBehaviorWritable> period) {
		UserBehaviorPattern ubp = new UserBehaviorPattern();

		return ubp;
	}
	
	public class BehaviorUnit
	{
		private int behaviorType;
		private int times;
		
		public BehaviorUnit(int behaviorType, int times) {
			super();
			this.setBehaviorType(behaviorType);
			this.setTimes(times);
		}

		public int getBehaviorType() {
			return behaviorType;
		}

		public void setBehaviorType(int behaviorType) {
			this.behaviorType = behaviorType;
		}

		public int getTimes() {
			return times;
		}

		public void setTimes(int times) {
			this.times = times;
		}
	}
}
