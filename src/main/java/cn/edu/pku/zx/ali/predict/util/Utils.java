package cn.edu.pku.zx.ali.predict.util;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import cn.edu.pku.zx.ali.predict.entity.UserBehaviorWritable;

public class Utils {
	public static void verifyOrder(Iterable<UserBehaviorWritable> ubs) throws Exception
	{
		UserBehaviorWritable pre=null;
		for(UserBehaviorWritable ub: ubs)
		{
			if(null!=pre&&pre.getTime().compareTo(ub.getTime())>0)
				throw new Exception("Wrong Order!!!!!!!!!!!!!!!!!!");
			pre=ub;
		}
	}
	
	public static List<UserBehaviorWritable> deepCopyAndSort(Iterable<UserBehaviorWritable> ubs)
	{
		List<UserBehaviorWritable> list_behaviors = new LinkedList<UserBehaviorWritable>();
		for (UserBehaviorWritable ub : ubs)				
			list_behaviors.add((UserBehaviorWritable) ub.clone());
						
		//sort ubw ascending
		Collections.sort(list_behaviors);
		
		return list_behaviors;
	}
}
