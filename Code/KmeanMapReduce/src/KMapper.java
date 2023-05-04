import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends Mapper<LongWritable, Text, LongWritable, PointWritable> {

	private PointWritable[] currCentroids; //tham số giữ vị trí trung tâm hiện tại
	private final LongWritable centroidId = new LongWritable(); //tham số lưu thông tin mã vị trí trung tâm (stt)
	private final PointWritable pointInput = new PointWritable();//Tham sô lưu điểm đầu vào

	@Override
	public void setup(Context context) { //hàm thực hiện setup phân cụm
		int nClusters = Integer.parseInt(context.getConfiguration().get("k"));//nếu k=3 thì có 3 cụm

		this.currCentroids = new PointWritable[nClusters];//lưu điểm dữ liệu hiện tại
		//ý nghĩa: từ context đầu vào, chuyển thể sang dạng phân cụm theo setup trước đó
		//-->Lấy các điểm trung tâm hiện tại của từng phân cụm
		for (int i = 0; i < nClusters; i++) {
			String[] centroid = context.getConfiguration().getStrings("C" + i);
			// this.currCentroids[i] = new PointWritable(centroid[0].split(","));
			this.currCentroids[i] = new PointWritable(centroid);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] arrPropPoint = value.toString().split(","); //Tách các điểm dữ liệu bằng dầu phẩy
		pointInput.set(arrPropPoint); //Đưa dữ liệu vào pointInput
		//Tìm ra cụm gần nhất
		double minDistance = Double.MAX_VALUE;
		int centroidIdNearest = 0;
		for (int i = 0; i < currCentroids.length; i++) {
			System.out.println("currCentroids[" + i + "]=" + currCentroids[i].toString());
			double distance = pointInput.calcDistance(currCentroids[i]);
			if (distance < minDistance) {
				centroidIdNearest = i;
				minDistance = distance;
			}
		}
		centroidId.set(centroidIdNearest); //lưu lại tâm cụm gần nhất của điểm dữ liệu 
		context.write(centroidId, pointInput); //đầu ra là <key,value>  =  <cụm thuộc về,điểm dữ liệu đó>
	}
}