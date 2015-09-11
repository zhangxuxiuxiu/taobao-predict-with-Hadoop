package cn.edu.pku.zx.ali.predict.model;

import java.util.LinkedList;
import java.util.List;

import cn.edu.pku.zx.ali.predict.entity.UserBehaviorWritable;
import cn.edu.pku.zx.ali.predict.util.Utils;

public abstract class BasePredictor implements IPredictable {
	private IntervalBasedPeriodCutter periodCutter=new IntervalBasedPeriodCutter();
	private static final int MIN_UNACTIVE_INTERVAL=1;
		
	
	@Override
	public final Iterable<Long> Predict(Iterable<UserBehaviorWritable> behaviors) {
		LinkedList<LinkedList<UserBehaviorWritable>> periods=periodCutter.cut(behaviors);
		return pickInPeriods(periods);
	}

	protected abstract Iterable<Long> pickInPeriods(
			LinkedList<LinkedList<UserBehaviorWritable>> periods) ;

	private class IntervalBasedPeriodCutter {
		/**
		 * @precondition: all UserBehaviorWritable objects are ordered by time
		 *                ascending.
		 * 
		 *                period partition: if two continuous user behaviors have
		 *                over 3 days apart, they are in different periods.
		 * 
		 * @return a list of periods and each period holds a list of user behaviors.
		 * @throws Exception 
		 */
		public LinkedList<LinkedList<UserBehaviorWritable>> cut( Iterable<UserBehaviorWritable> behaviors)  {
			List<UserBehaviorWritable> list_behaviors=Utils.deepCopyAndSort(behaviors);
			
			LinkedList<LinkedList<UserBehaviorWritable>> periods = new LinkedList<LinkedList<UserBehaviorWritable>>();

			UserBehaviorWritable pre_ub = null;
			LinkedList<UserBehaviorWritable> current_period = new LinkedList<UserBehaviorWritable>();
			for (UserBehaviorWritable current_ub : list_behaviors) {
				// when current UBW has a over 1-day interval between previous one,
				// period collection should be refreshed and a new period should created.
				if (null != pre_ub&& current_ub.getTime().IntervalBetween(pre_ub.getTime()) > MIN_UNACTIVE_INTERVAL) {//  general : >1
					periods.add(current_period);
					current_period = new LinkedList<UserBehaviorWritable>();
				}

				// refresh the previous UBW and current period
				current_period.add(current_ub);
				pre_ub = current_ub;
			}
			// add last period
			periods.add(current_period);

			return periods;
		}
	}
}
