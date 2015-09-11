package cn.edu.pku.zx.ali.predict.model;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.edu.pku.zx.ali.predict.config.Configuration;
import cn.edu.pku.zx.ali.predict.config.IConfigurable;
import cn.edu.pku.zx.ali.predict.entity.DateTimeWritable;
import cn.edu.pku.zx.ali.predict.entity.UserBehaviorWritable;

/**
 * phrase one: divide the last month into a few shopping periods;
 * 
 * phrase two: group the items by item_id;
 * 
 * phrase three: pick a few items from active items
 * 
 * @author zhangxu
 * 
 */
public final class RecentActivenessPredictor extends BasePredictor {
	private ItemIdGrouper grouper;
	private RecentActivenessPicker picker;
	private IConfigurable config;
	
	public RecentActivenessPredictor() {
		grouper = new ItemIdGrouper();
		picker = new RecentActivenessPicker();
	}

	/**
	 * predict items to buy tomorrow based history behavior in last month.
	 * 
	 * @return a list of item ids which the customer is likely to buy tomorrow.
	 * @throws Exception 
	 */
	@Override
	public Iterable<Long> pickInPeriods(LinkedList<LinkedList<UserBehaviorWritable>> periods){	
		//initialize configuration
		config=Configuration.getConfig();
		
		// obtain the last period
		LinkedList<UserBehaviorWritable> last_period = periods.getLast();

		// group by item_id
		Map<Long, LinkedList<UserBehaviorWritable>> item_behaviors_buckets = grouper.group(last_period);

		// pick items in each to-buy category
		return picker.pick(item_behaviors_buckets);
	}

	/**
	 * group UserBehaviorWritable objects by item_id
	 * 
	 * @author zhangxu
	 * 
	 */
	private class ItemIdGrouper {
		public Map<Long, LinkedList<UserBehaviorWritable>> group(
				LinkedList<UserBehaviorWritable> behaviors) {
			Map<Long, LinkedList<UserBehaviorWritable>> item_behaviors_buckets = new HashMap<Long, LinkedList<UserBehaviorWritable>>();

			for (UserBehaviorWritable ub : behaviors) {
				// if this item does not exist before, then create bucket for it
				if (!item_behaviors_buckets.containsKey(ub.getItem_id().get()))
					item_behaviors_buckets.put(ub.getItem_id().get(),
							new LinkedList<UserBehaviorWritable>());

				// add current UBW to its item bucket
				item_behaviors_buckets.get(ub.getItem_id().get()).add(ub);
			}

			return item_behaviors_buckets;
		}
	}

	/**
	 * if a certain item_id is active recently, it will be picked up
	 * 
	 * @author zhangxu
	 * 
	 */
	private class RecentActivenessPicker {
				
		public Iterable<Long> pick(
				Map<Long, LinkedList<UserBehaviorWritable>> item_behaviors_buckets) {
			List<Long> to_buy_items = new LinkedList<Long>();

			// analyze each item
			for (Entry<Long, LinkedList<UserBehaviorWritable>> entry : item_behaviors_buckets.entrySet()) {
				ItemsMsgPerPeriod msg = extractMsg(entry.getValue());
				if (msg.isActiveRecently())
					to_buy_items.add(entry.getKey());
			}

			return to_buy_items;
		}

		private ItemsMsgPerPeriod extractMsg(
				LinkedList<UserBehaviorWritable> activities) {
			ItemsMsgPerPeriod msg = new ItemsMsgPerPeriod();
			msg.end_date = activities.getLast().getTime();

			for (UserBehaviorWritable ub : activities)
				++msg.behaviors[ub.getBehavior_type().get() - 1];

			return msg;
		}
		
		private class ItemsMsgPerPeriod {
			private DateTimeWritable end_date;
			private int[] behaviors;
			
			public ItemsMsgPerPeriod() {
				end_date = null;
				behaviors = new int[4];
				for (int idx = 0; idx < behaviors.length; ++idx)
					behaviors[idx] = 0;
			}

			private boolean isActiveRecently() {
				if (end_date.IntervalBetween(DateTimeWritable.PredictionDate) >config.getMax_Inactive_Days())
					return false;

				if (0 != behaviors[UserBehaviorWritable.BEHAVIOR_TYPE_BUY - 1])
					return false;

				if (behaviors[UserBehaviorWritable.BEHAVIOR_TYPE_BROWSE - 1] >= config.getOrdinary_Times())
					return true;

				if (0 != behaviors[UserBehaviorWritable.BEHAVIOR_TYPE_CART - 1]
						&& behaviors[UserBehaviorWritable.BEHAVIOR_TYPE_BROWSE - 1] >= config.getMin_Times())
					return true;

				return false;
			}
		}

		
	}
}
