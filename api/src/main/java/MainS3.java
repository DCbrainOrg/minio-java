import java.text.NumberFormat;
import java.time.Duration;
import java.util.Random;

import io.minio.BucketExistsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectOutputStream;
import io.minio.PutObjectOutputStreamArgs;

public class MainS3 {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Expect 3 args: bucket accesskey secretKey");
			return;
		}
		String host = "https://s3.fr-par.scw.cloud";
		String bucket = args[0];
		String accesskey = args[1];
		String secretkey = args[2];
		String region = "fr-par";

		MinioClient minioClient = MinioClient.builder().endpoint(host).region(region).credentials(accesskey, secretkey).build();
		
		boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
		if (!found) {
			System.err.println("Could not found bucket " + bucket);
			return;
		}
		
		long size = 900 * 1024 * 1024;
		
		byte[] b = new byte[1 * 1024];
		long length = 0;
		long starTs = System.currentTimeMillis();
		Random random = new Random();

		NumberFormat formatter = NumberFormat.getNumberInstance();

		try (PutObjectOutputStream os = new PutObjectOutputStream(minioClient,
				PutObjectOutputStreamArgs.builder()
						.bucket(bucket)
						.object("async3.csv")
						.size(-1, 10 * 1024 * 1024)
						.maxParallelRequests(5)
						.build(),
				true)) {
			
			while (length < size) {
				random.nextBytes(b);
				os.write(b);
				length += b.length;
			}
			os.close();
			long durAsync = System.currentTimeMillis() - starTs;
			System.out.println("Async write in " + Duration.ofMillis(durAsync) + " "
					+ formatter.format((double) length  / 1024 /1024 / durAsync * 1000) + "B/s");
			os.getFutureClose().get();
			long dur = System.currentTimeMillis() - starTs;
			System.out.println(
					"write in " + Duration.ofMillis(dur) + " " + formatter.format((double) length / 1024 /1024 / dur * 1000) + "MB/s");
			System.out.println(
					"Waiting: " + Duration.ofMillis(os.getWaitingDuration()));
		}
	}
}
