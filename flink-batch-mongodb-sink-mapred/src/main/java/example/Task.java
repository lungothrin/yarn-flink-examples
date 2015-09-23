package example;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.bson.types.ObjectId;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.DBCollection;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import com.mongodb.hadoop.mapred.output.MongoOutputCommitter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;


class MongoOutputCommitterDelegate extends OutputCommitter
{
	private MongoOutputCommitter delegate;

	public MongoOutputCommitterDelegate() {
		MongoClientURI uri = new MongoClientURI("mongodb://127.0.0.1/test.junk");
		List<DBCollection> colls = new ArrayList<DBCollection>();
		colls.add(new MongoClient(uri).getDB(uri.getDatabase())
					      .getCollection(uri.getCollection()));
		delegate = new MongoOutputCommitter(colls);
	}

	/*
	public static Path getTaskAttemptPath(final TaskAttemptContext context) {
		return MongoOutputCommitter.getTaskAttemptPath(context);
	}
	*/

	@Override
	public void setupJob(final JobContext jobContext) {
		delegate.setupJob(jobContext);
	}

	@Override
	public void setupTask(final TaskAttemptContext taskContext)
			throws IOException {
		delegate.setupJob(taskContext);
	}

	@Override
	public boolean needsTaskCommit(final TaskAttemptContext taskContext)
			throws IOException {
		return delegate.needsTaskCommit(taskContext);
	}

	@Override
	public void commitTask(final TaskAttemptContext taskContext)
			throws IOException {
		delegate.commitTask(taskContext);
	}

	@Override
	public void abortTask(final TaskAttemptContext taskContext)
			throws IOException {
		delegate.abortTask(taskContext);
	}
};

public class Task {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env
			= ExecutionEnvironment.getExecutionEnvironment();

		JobConf conf = new JobConf();
		conf.setOutputCommitter(MongoOutputCommitterDelegate.class);
		HadoopOutputFormat<ObjectId, BSONWritable> outputFormat = 
			new HadoopOutputFormat<ObjectId, BSONWritable>(
				new MongoOutputFormat<ObjectId, BSONWritable>(),
				conf);

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
					   Tuple2<ObjectId, BSONWritable>>() {
				@Override
				public Tuple2<ObjectId, BSONWritable>
					map(Tuple3<Integer, Integer, Integer> tuple)
				{
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
