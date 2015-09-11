package cn.edu.pku.zx.ali.predict.model;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cn.edu.pku.zx.ali.predict.entity.UserBehaviorWritable;

public class PatternBasedPredictor extends BasePredictor {
	private ItemCategoryGrouper grouper;
	private UserBehaviorPatternAnalyzer patternAnalyzer;
	private PatternBasedPicker picker;
	
	public PatternBasedPredictor()
	{
		grouper=new ItemCategoryGrouper();
		patternAnalyzer=new UserBehaviorPatternAnalyzer();
		picker=new PatternBasedPicker();
	}
	
	@Override
	public Iterable<Long> pickInPeriods(LinkedList<LinkedList<UserBehaviorWritable>> periods){
		//group by item category
		LinkedList<Map<Long, LinkedList<UserBehaviorWritable>>> category_behaviors_buckets_periods = grouper.group(periods);
		
		Map<Long, LinkedList<UserBehaviorWritable>> last_period=category_behaviors_buckets_periods.getLast();
		UserShoppingPattern pattern=patternAnalyzer.analyze(category_behaviors_buckets_periods);
		
		return picker.pick(last_period,pattern);
	}

	private class ItemCategoryGrouper
	{
		/**
		 * put items in each item category in different buckets.
		 * 
		 * @return a list of periods and in each period every item category acts as
		 *         key and its items act as value with time ascending.
		 */
		public LinkedList<Map<Long, LinkedList<UserBehaviorWritable>>> group(
				LinkedList<LinkedList<UserBehaviorWritable>> shopping_periods) {
			LinkedList<Map<Long, LinkedList<UserBehaviorWritable>>> periods_group_by_category = new LinkedList<Map<Long, LinkedList<UserBehaviorWritable>>>();

			for (List<UserBehaviorWritable> period : shopping_periods) {
				// for each period, group UserBehaviorWritable objects by category
				Map<Long, LinkedList<UserBehaviorWritable>> period_category_items = new HashMap<Long, LinkedList<UserBehaviorWritable>>();

				for (UserBehaviorWritable ub : period) {
					// if this category does not exist before, then create bucket for it
					if (!period_category_items.containsKey(ub.getItem_category().get()))
						period_category_items.put(ub.getItem_category().get(),new LinkedList<UserBehaviorWritable>());

					// add current UBW to its category bucket
					period_category_items.get(ub.getItem_category().get()).add(ub);
				}

				// refresh period collections
				periods_group_by_category.add(period_category_items);
			}

			return periods_group_by_category;
		}	
	}
	
	private class UserBehaviorPatternAnalyzer
	{
		/**
		 * analyze the user shopping habit based on user's history shopping behavior
		 * 
		 * @param categoriy_items_periods
		 *            : a list of periods. in each period, UBW is grouped by item category
		 * 
		 * @return a UserShoppingHabit object which depicts the user's habit
		 */
		public  UserShoppingPattern analyze(
				LinkedList<Map<Long, LinkedList<UserBehaviorWritable>>> category_items_periods) {
			UserShoppingPattern ush = new UserShoppingPattern();

			return ush;
		}	
	}

	/**
	 * construct a series of user's habits based on its history shopping
	 * behaviors.
	 * 
	 * @author zhangxu
	 * 
	 */
	private class UserShoppingPattern {
//		private double avg_interval;// average interval between continuous
//									// shopping periods
//		private double deviation_interval; // standard deviation of all shopping
//											// period intervals
//		private double avg_duration;// average duration each shopping period
//									// lasted
//		private double deviation_duration;// standard deviation of all shopping
//											// periods
//		private double avg_goods_per_period;// average goods in each period
//		private double deviation_goods_per_period;// standard deviation of goods
//													// in each period
//		private double avg_observation_days_before_shopping; // average
//																// observation
//																// days before
//																// actually
//																// buying a
//																// certain goods
//		private double deviation_observation_days_before_shopping; // standard
//																	// deviation
//																	// of
//																	// observation
//																	// days
//																	// before
//																	// actually
//																	// buying a
//																	// certain
//																	// goods
//		private double shopping_ratio;// goods bought divided by goods observed
	}
	
	private class PatternBasedPicker {
		public Iterable<Long> pick(
				Map<Long, LinkedList<UserBehaviorWritable>> last_period,
				UserShoppingPattern pattern) {
			
			return null;
		}
	}
}
