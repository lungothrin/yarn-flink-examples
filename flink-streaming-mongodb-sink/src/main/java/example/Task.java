package example;

import org.bson.Document;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.MongoOutput;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


class MongoSink<K, V> extends RichSinkFunction<Tuple2<K, V>> {
	private MongoCollection<Document> coll;

	@Override
	public void invoke(Tuple2<K, V> value) {
		K key = value.getField(0);
		V val = value.getField(1);

		if (val instanceof MongoUpdateWritable) {
			MongoUpdateWritable o = (MongoUpdateWritable)val;
			coll.updateOne(new BasicDBObject(o.getQuery().toMap()),
				       new BasicDBObject(o.getModifiers().toMap()));
		} else {
			DBObject o = new BasicDBObject();
			if (key instanceof BSONWritable) {
				o.put("_id", ((BSONWritable) key).getDoc());
			} else if (key instanceof BSONObject) {
				o.put("_id", key);
			} else {
				o.put("_id", BSONWritable.toBSON(key));
			}

			if (val instanceof BSONWritable) {
				o.putAll(((BSONWritable)val).getDoc());
			} else if (value instanceof MongoOutput) {
				((MongoOutput)val).appendAsValue(o);
			} else if (value instanceof BSONObject) {
				o.putAll((BSONObject)val);
			} else {
				o.put("value", BSONWritable.toBSON(val));
			}
			coll.insertOne(new Document(o.toMap()));
		}
	}

	@Override
	public void open(Configuration config) {
		MongoClientURI uri = new MongoClientURI(
				config.getString("mongo.output.uri",
						 "mongodb://127.0.0.1/test.junk"));
		coll = new MongoClient(uri).getDatabase(uri.getDatabase())
					   .getCollection(uri.getCollection());
	}
};

public class Task {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env
			= StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple3<Integer, Integer, Integer>> source
			= env.fromElements(
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
				public Tuple2<ObjectId, BSONWritable>
					map(Tuple3<Integer, Integer, Integer> tuple)
				{
					return new Tuple2<ObjectId, BSONWritable>(
						new ObjectId(),
						new BSONWritable(
							BasicDBObjectBuilder.start()
								.add("id", tuple.getField(0))
								.add("lower", tuple.getField(1))
								.add("upper", tuple.getField(2))
								.get()
							)
						);
				}
			   })
		      .addSink(new MongoSink<ObjectId, BSONWritable>());

		env.execute();
	}
}
