package cn.edu.pku.zx.ali.predict.evaluate;

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

import cn.edu.pku.zx.ali.predict.entity.DateTimeWritable;
import cn.edu.pku.zx.ali.predict.entity.LongPairWritable;
import cn.edu.pku.zx.ali.predict.entity.UserBehaviorWritable;

public class ResultSetBuilder extends Configured implements Tool {

	private static class ResultSetFilter extends
			Mapper<LongWritable, Text, LongPairWritable, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			UserBehaviorWritable ub = UserBehaviorWritable.getInstance(value.toString());
			if (ub.getTime().IntervalBetween(DateTimeWritable.PredictionDate) == 0
					&& ub.getBehavior_type().get() == UserBehaviorWritable.BEHAVIOR_TYPE_BUY) {
				LongWritable user_id = new LongWritable(ub.getUser_id().get()), 
						     item_id = new LongWritable(ub.getItem_id().get());
				context.write(new LongPairWritable(user_id, item_id), NullWritable.get());
			}
		}
	}

	private static class Uniquer extends
			Reducer<LongPairWritable, NullWritable, LongPairWritable, NullWritable> {
		@Override
		protected void reduce(LongPairWritable key, Iterable<NullWritable> ubs,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		// setting job name
		Job job = Job.getInstance(getConf(), "Result Set Builder");
		job.setJarByClass(ResultSetBuilder.class);

		// setting job input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// setting mapper for the MR job
		job.setMapperClass(ResultSetFilter.class);
		job.setReducerClass(Uniquer.class);
		
		// setting input and output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// setting the k-v class of mapping and reducing
		job.setMapOutputKeyClass(LongPairWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(LongPairWritable.class);
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ResultSetBuilder(), args);
		System.exit(exitCode);
	}
}
