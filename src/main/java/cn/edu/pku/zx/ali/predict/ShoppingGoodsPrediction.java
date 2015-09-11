package cn.edu.pku.zx.ali.predict;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.edu.pku.zx.ali.predict.entity.LongPairWritable;
import cn.edu.pku.zx.ali.predict.entity.UserBehaviorWritable;
import cn.edu.pku.zx.ali.predict.model.IPredictable;
import cn.edu.pku.zx.ali.predict.model.RecentActivenessPredictor;

/**
 * @Problem Discription:训练数据包含了抽样出来的一定量用户在一个月时间（11.18~12.18）之内的移动端行为数据（D），
 *          评分数据是这些用户在这个一个月之后的一天 （12.19）对商品子集（P）的购买数据。参赛者要使用训练数据
 *          建立推荐模型，并输出用户在接下来一天对商品子集购买行为的预测结果。
 * 
 * 
 */
public class ShoppingGoodsPrediction extends Configured implements Tool {
	/**
	 * mapping a string to a UserBehavior object
	 * 
	 * @author zhangxu
	 * 
	 */
	private static class LineToUserBehaviorMapper extends
			Mapper<LongWritable, Text, LongWritable, UserBehaviorWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			UserBehaviorWritable ub = UserBehaviorWritable.getInstance(value.toString());
			//if(ub.getTime().compareTo(DateTimeWritable.PredictionDate)<0)
				context.write(ub.getUser_id(), ub);
		}
	}

	/**
	 * construct a prediction model from a list of user behaviors in one month
	 * and predict which subset the user will buy in the following day
	 * 
	 * @author zhangxu
	 * 
	 */
	private static class UserBehaviorPredictor extends
			Reducer<LongWritable, UserBehaviorWritable, LongPairWritable, NullWritable> {
		@Override
		protected void reduce(LongWritable key, Iterable<UserBehaviorWritable> ubs,
				Context context) throws IOException, InterruptedException {
			IPredictable predictor = new RecentActivenessPredictor();
			//for each item the predictor predicts, write them into output						
			Iterable<Long> items = predictor.Predict(ubs);
			for (Long item_id : items)
				context.write(new LongPairWritable(key, new LongWritable(item_id.longValue())),NullWritable.get());	
		}
	}

//	/**
//	 * sort user_id only
//	 * @author zhangxu
//	 *
//	 */
//	public static class UserSorterComparator extends WritableComparator
//	{
//		private static final IntWritable.Comparator INT_COMPARATOR=new IntWritable.Comparator();
//		private static final int INT_SIZE=Integer.SIZE/8;
//		
//		public UserSorterComparator()
//        {
//            super(UserBehaviorWritable.class,true);
//        }
//
//		@Override
//		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//			return INT_COMPARATOR.compare(b1, s1, INT_SIZE, b2, s2, INT_SIZE);
//		}
//	}
//	
//	/**
//	 * hash UserBehaviorWritable by user_id
//	 * @author zhangxu
//	 *
//	 */
//	public static class UserBasedPartitioner extends Partitioner<UserBehaviorWritable,UserBehaviorWritable>
//    {
//		@Override
//		public int getPartition(UserBehaviorWritable key,UserBehaviorWritable value,int numPartitions) {
//			return Math.abs(key.getUser_id().hashCode()*127)%numPartitions;
//		}
//    }
//	
//	/**
//	 * group UserBehaviorWritable by user_id
//	 * @author zhangxu
//	 *
//	 */
//	public static class GroupByUserComparator extends WritableComparator
//    {
//		private static final LongWritable.Comparator LONG_COMPARATOR=new LongWritable.Comparator();
//		private static final int LONG_SIZE=Long.SIZE/8;
//		
//        protected GroupByUserComparator()
//        {
//            super(UserBehaviorWritable.class,true);
//        }
//
//        @Override
//        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//			return LONG_COMPARATOR.compare(b1, s1, LONG_SIZE, b2, s2,LONG_SIZE);
//		}
//    }
	
	
	/**
	 * construct a map-reduce job and run it
	 */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		// setting job name
		Job job = Job.getInstance(getConf(), "Shopping Goods Prediction");
		job.setJarByClass(ShoppingGoodsPrediction.class);

		// setting job input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// setting mapper, reducer, sorter, partitioner and grouper for the MR job
		job.setMapperClass(LineToUserBehaviorMapper.class);
		job.setReducerClass(UserBehaviorPredictor.class);
//		job.setSortComparatorClass(UserSorterComparator.class);
//		job.setPartitionerClass(UserBasedPartitioner.class);
//		job.setGroupingComparatorClass(GroupByUserComparator.class);
		
		// setting input and output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// setting the k-v class of mapping and reducing
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(UserBehaviorWritable.class);
		job.setOutputKeyClass(LongPairWritable.class);
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ShoppingGoodsPrediction(), args);
		System.exit(exitCode);
	}
}
