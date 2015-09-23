package example;

import org.bson.types.ObjectId;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.MongoOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;


public class Task {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env
			= ExecutionEnvironment.getExecutionEnvironment();

		Configuration conf = new Configuration();
		conf.set("mapred.output.dir", "/tmp");
		conf.set("mongo.output.uri", "mongodb://127.0.0.1/test.junk");

		HadoopOutputFormat<ObjectId, BSONWritable> outputFormat = 
			new HadoopOutputFormat<ObjectId, BSONWritable>(
				new MongoOutputFormat<ObjectId, BSONWritable>(),
				Job.getInstance(conf));

		DataSet<Tuple3<Integer, Integer, Integer>> source = 
			env.fromElements(
				new Tuple3<Integer, Integer, Integer>(0, 1, 2),
				new Tuple3<Integer, Integer, Integer>(1, 2, 3),
				new Tuple3<Integer, Integer, Integer>(2, 3, 4),
				new Tuple3<Integer, Integer, Integer>(3, 4, 5),
				new Tuple3<Integer, Integer, Integer>(4, 5, 6),
				new Tuple3<Integer, Integer, Integer>(5, 6, 7),
				new Tuple3<Integer, Integer, Integer>(6, 7, 8)
			);

		source.map(new MapFunction<Tuple3<Integer, Integer, Integer>,
					    Tuple2<ObjectId, BSONWritable>>()
			    {
				@Override
				public Tuple2<ObjectId, BSONWritable> map(Tuple3<Integer, Integer, Integer> tuple) {
					return new Tuple2<ObjectId, BSONWritable>(
						new ObjectId(),
						new BSONWritable(BasicDBObjectBuilder.start()
							.add("id", tuple.getField(0))
							.add("lower", tuple.getField(1))
							.add("upper", tuple.getField(2))
							.get())
						);
				}
			    })
		      .output(outputFormat);

		env.execute();
	}
}
