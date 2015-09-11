package cn.edu.pku.zx.ali.predict.model;

import cn.edu.pku.zx.ali.predict.entity.UserBehaviorWritable;

/**
 * This interface is used for predicting which subsets of goods a certain
 * customer will buy in the following one day based on one month of shopping
 * behaviors history
 * 
 * @author zhangxu
 * 
 */
public interface IPredictable {
	/**
	 * predict which subset goods the customer will buy tomorrow
	 * 
	 * @param behaviors
	 *            : customer's history shopping behaviors in last month
	 * @return a list of item_ids which the customer will buy
	 * @throws Exception 
	 */
	public Iterable<Long> Predict(Iterable<UserBehaviorWritable> behaviors);
}
