import java.awt.Point;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aliyun.oss.common.utils.IOUtils;
import com.cedarsoftware.util.ReflectionUtils;

public class Main extends Configured implements Tool {
	
	public static PointWritable[] readPointFromHDFS(String filePath) {
	    Configuration conf = new Configuration();
	    Path path = new Path(filePath);

	    try {
	        FileSystem fs = FileSystem.get(path.toUri(), conf);
	        InputStream inputStream = fs.open(path);
	        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

	        List<PointWritable> points = new ArrayList<>();
	        String line;

	        while ((line = bufferedReader.readLine()) != null) {
	            String[] values = line.split(",");
	            float[] coords = new float[values.length];
	            for (int i = 0; i < values.length; i++) {
	                coords[i] = Float.parseFloat(values[i].trim());
	            }
	            PointWritable point = new PointWritable(coords);
	            points.add(point);
	        }

	        bufferedReader.close();
	        return points.toArray(new PointWritable[points.size()]);

	    } catch (IOException e) {
	        System.err.println("Error reading HDFS file: " + e.getMessage());
	        return null;
	    }
	}


	public static double calculateDBI(PointWritable[] dataPoints, PointWritable[] centroids) {
	    int k = centroids.length;
	    double[] sigma = new double[k];
	    double[][] dist = new double[k][k];
	    double dbi = 0.0;

	    // Tính toán khoảng cách giữa các điểm trung tâm
	    for (int i = 0; i < k; i++) {
	        for (int j = i + 1; j < k; j++) {
	            dist[i][j] = centroids[i].calcDistance(centroids[j]);
	            dist[j][i] = dist[i][j];
	        }
	    }

	    // Tính toán độ phân tán của từng cụm
	    for (int i = 0; i < k; i++) {
	        double sum = 0.0;
	        for (PointWritable point : dataPoints) {
	            if (point.getCluster() == i) {
	                sum += point.calcDistance(centroids[i]);
	            }
	        }
	        sigma[i] = sum / countClusterPoints(dataPoints, i);
	    }

	    // Tính toán chỉ số DBI
	    for (int i = 0; i < k; i++) {
	        double max = Double.NEGATIVE_INFINITY;
	        for (int j = 0; j < k; j++) {
	            if (i != j) {
	                double db = (sigma[i] + sigma[j]) / dist[i][j];
	                if (db > max) {
	                    max = db;
	                }
	            }
	        }
	        dbi += max;
	    }

	    return dbi / k;
	}

	// Hàm đếm số điểm trong một cụm
	private static int countClusterPoints(PointWritable[] dataPoints, int cluster) {
	    int count = 0;
	    for (PointWritable point : dataPoints) {
	        if (point.getCluster() == cluster) {
	            count++;
	        }
	    }
	    return count;
	}


	
	
	
	//Hàm khởi tạo các điểm tâm cụm ban đầu ngẫu nhiên
	public static PointWritable[] initRandomCentroids(int kClusters, int nLineOfInputFile, String inputFilePath,
			Configuration conf) throws IOException {
		System.out.println("Initializing random " + kClusters + " centroids...");
		PointWritable[] points = new PointWritable[kClusters]; //lưu các điểm tâm cụm

		List<Integer> lstLinePos = new ArrayList<Integer>();//lưu thứ tự các điểm được chọn làm tâm cụm vd: arr=[1,20,30,22...]
		Random random = new Random();
		int pos;//lưu vị trí của điểm
		while (lstLinePos.size() < kClusters) {
			pos = random.nextInt(nLineOfInputFile);//khởi tạo vị trí ngẫu nhiên trong danh sách các cụm
			if (!lstLinePos.contains(pos)) {
				//Nếu như điểm khởi tạo không bị trùng thì thêm vào danh sách các tâm cụm
				lstLinePos.add(pos);
			}
		}
		Collections.sort(lstLinePos); //sắp xếp lại danh sách tâm cụm ???

		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream in = hdfs.open(new Path(inputFilePath)); //input là tên đường dẫn file

		BufferedReader br = new BufferedReader(new InputStreamReader(in));  //đọc file

		int row = 0;
		int i = 0;
		while (i < lstLinePos.size()) {
			pos = lstLinePos.get(i); //Lấy ra vị trí điểm của tâm cụm thứ i
			String point = br.readLine();//lấy ra dữ liệu điểm
			if (row == pos) {//Nếu hàng trùng với vị trí thì lưu lại giá trị của các tâm cụm
				points[i] = new PointWritable(point.split(","));
				i++;
			}
			row++;
		}
		br.close();
		return points;// trả về danh sách giá trị của tâm cụm
	}

	public static void saveCentroidsForShared(Configuration conf, PointWritable[] points) {
		//Lưu lại các tâm cụm để chia cho các node cho việc tính khoảng cách tới các tâm cụm
		for (int i = 0; i < points.length; i++) {
			String centroidName = "C" + i;
			conf.unset(centroidName);
			conf.set(centroidName, points[i].toString());
		}
	}
	//Hàm đọc các tâm cụm từ kết quả trả về của hàm reduce
	public static PointWritable[] readCentroidsFromReducerOutput(Configuration conf, int kClusters,
			String folderOutputPath) throws IOException, FileNotFoundException {
		PointWritable[] points = new PointWritable[kClusters];
		FileSystem hdfs = FileSystem.get(conf);
		FileStatus[] status = hdfs.listStatus(new Path(folderOutputPath));

		for (int i = 0; i < status.length; i++) {

			if (!status[i].getPath().toString().endsWith("_SUCCESS")) {
				Path outFilePath = status[i].getPath();
				System.out.println("read " + outFilePath.toString());
				BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(outFilePath)));
				String line = null;// br.readLine();
				while ((line = br.readLine()) != null) {
					System.out.println(line);

					String[] strCentroidInfo = line.split("\t"); // Split line in K,V
					int centroidId = Integer.parseInt(strCentroidInfo[0]);
					String[] attrPoint = strCentroidInfo[1].split(",");
					points[centroidId] = new PointWritable(attrPoint);
				}
				br.close();
			}
		}

		hdfs.delete(new Path(folderOutputPath), true);

		return points;
	}

	private static boolean checkStopKMean(PointWritable[] oldCentroids, PointWritable[] newCentroids, float threshold) {
		boolean needStop = true;

		System.out.println("Check for stop K-Means if distance <= " + threshold);
		for (int i = 0; i < oldCentroids.length; i++) {

			double dist = oldCentroids[i].calcDistance(newCentroids[i]);
			System.out.println("distance centroid[" + i + "] changed: " + dist + " (threshold:" + threshold + ")");
			needStop = dist <= threshold;
			// chỉ cần 1 tâm > ngưỡng thì return false=> chương trình tiếp tục chạy
			if (!needStop) {
				return false;
			}
		}
		return true;
	}

	private static void writeFinalResult(Configuration conf, PointWritable[] centroidsFound, String outputFilePath,
		PointWritable[] centroidsInit) throws IOException {
		FileSystem hdfs = FileSystem.get(conf);
		FSDataOutputStream dos = hdfs.create(new Path(outputFilePath), true);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

		for (int i = 0; i < centroidsFound.length; i++) {
			br.write(centroidsFound[i].toString());
			br.newLine();
			System.out.println("Centroid[" + i + "]:  (" + centroidsFound[i] + ")  init: (" + centroidsInit[i] + ")");
		}

		br.close();
		hdfs.close();
	}
	
	//Hàm sao chép mảng lưu điểm dữ liệu
	public static PointWritable[] copyCentroids(PointWritable[] points) {
		PointWritable[] savedPoints = new PointWritable[points.length];
		for (int i = 0; i < savedPoints.length; i++) {
			savedPoints[i] = PointWritable.copy(points[i]);
		}
		return savedPoints;
	}

	public static int MAX_LOOP = 50;

	public static void printCentroids(PointWritable[] points, String name) {
		System.out.println("=> CURRENT CENTROIDS:");
		for (int i = 0; i < points.length; i++)
			System.out.println("centroids(" + name + ")[" + i + "]=> :" + points[i]);
		System.out.println("----------------------------------");
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		String inputFilePath = conf.get("in", null); //Cấu hình đường dẫn file đầu vào 
		String outputFolderPath = conf.get("out", null); //Cấu hình đường dẫn thư mục đầu ra
		String outputFileName = conf.get("result", "result.txt"); // tên file output mặc định là result.txt nếu không truyền vào
		int nClusters = conf.getInt("k", 3); //Khóa cấu hình là 'k' lấy ra số cụm, mặc định là 3
		float thresholdStop = conf.getFloat("thresh", 0.001f); // lấy ra ngưỡng dừng thresholdStop , mặc định là 0.001 kiểu số thực
		int numLineOfInputFile = conf.getInt("lines", 0); //Số dòng dữ liệu, mặc định là  0 
		MAX_LOOP = conf.getInt("maxloop", 50); // Vòng lặp tối đa, mặc định là 50
		int nReduceTask = conf.getInt("NumReduceTask", 1); //số tác vụ reduce , mặc định là 1
		
		//Thông báo lỗi nếu chạy lệnh thiếu
		if (inputFilePath == null || outputFolderPath == null || numLineOfInputFile == 0) { 
			System.err.printf(
					"Usage: %s -Din <input file name> -Dlines <number of lines in input file> -Dout <Folder ouput> -Dresult <output file result> -Dk <number of clusters> -Dthresh <Threshold>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		//In các tham số đầu vào 
		System.out.println("---------------INPUT PARAMETERS---------------");
		System.out.println("inputFilePath:" + inputFilePath);
		System.out.println("outputFolderPath:" + outputFolderPath);
		System.out.println("outputFileName:" + outputFileName);
		System.out.println("maxloop:" + MAX_LOOP);
		System.out.println("numLineOfInputFile:" + numLineOfInputFile);
		System.out.println("nClusters:" + nClusters);
		System.out.println("threshold:" + thresholdStop);
		System.out.println("NumReduceTask:" + nReduceTask);

		//Bắt đầu thực hiện chương trình chính
		System.out.println("--------------- STATR ---------------");
		PointWritable[] oldCentroidPoints = initRandomCentroids(nClusters, numLineOfInputFile, inputFilePath, conf);
		//Tạo mảng lưu các điểm khởi tạo, tham số đầu vào: số cụm, số dòng dữ liệu, tên file data , cấu hình  cụm
		PointWritable[] centroidsInit = copyCentroids(oldCentroidPoints);
		//Tạo mảng khác sao chép lại các điểm khởi tạo
		printCentroids(oldCentroidPoints, "init");
		//In ra các điểm tâm cụm đã khởi tạo
		
		saveCentroidsForShared(conf, oldCentroidPoints); //Lưu vào cấu hình các điểm tâm cụm 
		int nLoop = 0;

		PointWritable[] newCentroidPoints = null; //Tạo mảng mới cho việc lưu các tâm cụm mới 
		long t1 = (new Date()).getTime(); //Lưu thời gian bắt đầu chạy vòng lặp
		while (true) {
			nLoop++;
			if (nLoop == MAX_LOOP) {
				break;
			}
			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "K-Mean");
			job.setJarByClass(Main.class); 
			job.setMapperClass(KMapper.class);
			job.setCombinerClass(KCombiner.class);
			job.setReducerClass(KReducer.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(PointWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			//Cấu hình đường dẫn đầu vào và đầu ra cho một công việc mapreduce
			FileInputFormat.addInputPath(job, new Path(inputFilePath)); 
			FileOutputFormat.setOutputPath(job, new Path(outputFolderPath));
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setNumReduceTasks(nReduceTask);

			boolean ret = job.waitForCompletion(true); //chờ cho đến khi công việc MapReduce hoàn thành và trả về true 
			if (!ret) {
				return -1; 

			}
			//Cập nhật tâm cụm mới từ kết quả tác vụ Reduce
			newCentroidPoints = readCentroidsFromReducerOutput(conf, nClusters, outputFolderPath);
			
			printCentroids(newCentroidPoints, "new"); 
			//Kiểm tra điều kiện dừng
			boolean needStop = checkStopKMean(newCentroidPoints, oldCentroidPoints, thresholdStop);
			//lưu các tâm vừa dịch chuyển làm tâm cũ 
			oldCentroidPoints = copyCentroids(newCentroidPoints);

			if (needStop) { 
				break;
			} else {
				saveCentroidsForShared(conf, newCentroidPoints);
			}

		}
		//Gán nhãn cho tất cả các điểm
		PointWritable[] dataPoints = readPointFromHDFS(inputFilePath);
		for(int i=0;i< numLineOfInputFile; i++) {
			double minDistance = Double.MAX_VALUE;
			for (int j = 0; j < newCentroidPoints.length; j++) {
				double distance = dataPoints[i].calcDistance(newCentroidPoints[j]);
				if (distance < minDistance) {
					dataPoints[i].setCluster(j);
					minDistance = distance;
				}
			}
		}
		if (newCentroidPoints != null) { //Ghi kết quả ra file
			System.out.println("------------------- FINAL RESULT -------------------");
			writeFinalResult(conf, newCentroidPoints, outputFolderPath + "/" + outputFileName, centroidsInit);
		}
		System.out.println("----------------------------------------------");
		System.out.println("K-MEANS CLUSTERING FINISHED!");
		System.out.println("Loop:" + nLoop);
		System.out.println("Time:" + ((new Date()).getTime() - t1) + "ms"); 
		System.out.println("DBI:" + calculateDBI(dataPoints,newCentroidPoints)); 
		return 1;
	}

	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Main(), args);
		System.exit(exitCode);
	}
}













