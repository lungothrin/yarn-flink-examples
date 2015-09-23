package example;

import java.lang.ClassNotFoundException;
import java.lang.reflect.Type;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;;

import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.GsonBuilder;
import com.google.gson.Gson;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;


public class Task {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env
			= ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> source = env.fromElements(
					"{\"id\":0,\"lower\":1,\"upper\":2}",
					"{\"id\":1,\"lower\":2,\"upper\":3}",
					"{\"id\":2,\"lower\":3,\"upper\":4}",
					"{\"id\":3,\"lower\":4,\"upper\":5}",
					"{\"id\":4,\"lower\":5,\"upper\":6}",
					"{\"id\":5,\"lower\":6,\"upper\":7}",
					"{\"id\":6,\"lower\":7,\"upper\":8}"
				);

		source.map(new MapFunction<String, Tuple3<Integer, Integer, Integer>>() {
				private Gson json_;

				{
					initialize();
				}

				private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
					initialize();
				}

				private void writeObject(ObjectOutputStream out) throws IOException {
				}

				@Override
				public Tuple3<Integer, Integer, Integer> map(String raw) {
					return json_.fromJson(raw, new Tuple3<Integer, Integer, Integer>().getClass());
				}

				private void initialize() {
					GsonBuilder builder = new GsonBuilder();
					builder.registerTypeAdapter(new Tuple3<Integer, Integer, Integer>().getClass(),
								    new JsonDeserializer<Tuple3<Integer, Integer, Integer>>()
								    {
									public Tuple3<Integer, Integer, Integer>
									deserialize(JsonElement json,
										    Type type,
										    JsonDeserializationContext ctx)
										throws JsonParseException
									{
										Tuple3<Integer, Integer, Integer> res =
											new Tuple3<Integer, Integer, Integer>(0, 0, 0);

										if (!json.isJsonObject()) {
											return res;
										}

										JsonObject obj = json.getAsJsonObject();
										try {
											if (obj.has("id")) {
												JsonElement rid = obj.get("id");
												if (rid.isJsonPrimitive()) {
													res.setField(rid.getAsInt(), 0);
												}
											}
											if (obj.has("lower")) {
												JsonElement lower = obj.get("lower");
												if (lower.isJsonPrimitive()) {
													res.setField(lower.getAsInt(), 1);
												}
											}
											if (obj.has("upper")) {
												JsonElement upper = obj.get("upper");
												if (upper.isJsonPrimitive()) {
													res.setField(upper.getAsInt(), 2);
												}
											}
										} catch (JsonParseException except) {
										}

										return res;
									}
								    });
					json_ = builder.create();
				}
			   })
			.writeAsCsv("hdfs://127.0.0.1:9000/tmp/flink-gson.csv", FileSystem.WriteMode.OVERWRITE);

		env.execute();
	}
}
