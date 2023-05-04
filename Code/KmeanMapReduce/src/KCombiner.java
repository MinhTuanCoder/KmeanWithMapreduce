import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KCombiner extends Reducer<LongWritable, PointWritable, LongWritable, PointWritable> {

	public void reduce(LongWritable centroidId, Iterable<PointWritable> points, Context context)
			throws IOException, InterruptedException {

		PointWritable ptSum = PointWritable.copy(points.iterator().next());
		while (points.iterator().hasNext()) {
			ptSum.sum(points.iterator().next()); //tính tổng các point trong centroidID
		}

		context.write(centroidId, ptSum);//đầu ra là <cụm đó,tổng số điểm trong cụm>
	}
}