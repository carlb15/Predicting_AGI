import java.io.*;

public class  LsTest {
	public static void main(String[] args) throws Exception {
		                /**
                val hadoopConf = spark.sparkContext.hadoopConfiguration
                val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

                try {
                        val inputExists = fs.exists(new org.apache.hadoop.fs.Path(inputFilePath))
                        if (!inputExists) {
                                System.err.println("Input not found")
                                //System.exit(2)
                        }
                        val output1Exists = fs.exists(new org.apache.hadoop.fs.Path(coefFilePath))
                        if (output1Exists) {
                                System.err.println("Coefficients output path points to an already existing file/directory. Please run again with a free path")
                                //System.exit(3)
                        }
                        val output2Exists = fs.exists(new org.apache.hadoop.fs.Path(predFilePath))
                        if (output2Exists) {
                                System.err.println("Predictions output path points to an already existing file/directory. Please run again with a free path")
                                //System.exit(4)
                        }
                } catch {
                        case e: Exception => {
                                System.err.println("Something wrong with input/output paths, please correct and try again")
                                //System.exit(5)
                        }
                }
                **/
		ProcessBuilder builder = new ProcessBuilder("ls");
		builder.redirectErrorStream(true);
		Process p = builder.start();
		BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		while(true) {
			line = r.readLine();
			if (line == null) break;
			System.out.println(line);
		}
			
		//val result = "spark2-submit --name \"ModelTrainer\" --class ModelTrainer --master yarn --deploy-mode cluster --verbose target/scala-2.11/modeltrainer3_2.11-0.1.0-SNAPSHOT.jar /user/mlu216/SHARE/DF/combinedAvgAgiAndEdu.parquet /user/mlu216/SHARE/OUTPUT/7_18/coef1.parquet /user/mlu216/SHARE/OUTPUT/7_18/pred1.parquet" !
	}
}
