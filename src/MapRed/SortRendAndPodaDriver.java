package MapRed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import entities.CoordenadaConRendWritable;

public class SortRendAndPodaDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");

//		System.setProperty("anchoCelda", "0.0001");

//		Context context;
//		int neighbors = context.getConfiguration().getInt("neighbors", 2);

		job.setJarByClass(SortRendAndPodaDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(FirstMapper_GroupCoordRend.class);
		// TODO: specify a reducer
		job.setReducerClass(FirstReducer_SortAndPoda.class);

		// TODO: specify output types
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(CoordenadaConRendWritable.class);

		// TODO: specify input and output DIRECTORIES
		FileInputFormat.setInputPaths(job, new Path(
				"/home/pruebahadoop/Documentos/DataSets/monitores/input/monitores2.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/home/pruebahadoop/Documentos/DataSets/monitores/sortAndPoda"));

		if (!job.waitForCompletion(true))
			return;
	}

}
