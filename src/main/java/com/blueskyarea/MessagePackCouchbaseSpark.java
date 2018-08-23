package com.blueskyarea;

import static com.couchbase.spark.japi.CouchbaseDocumentRDD.couchbaseDocumentRDD;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.SerializableDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.spark.StoreMode;

public class MessagePackCouchbaseSpark {
	private static JavaSparkContext jsc;
	
	public MessagePackCouchbaseSpark() {
		SparkConf sparkConf = new SparkConf() {
            {
                setAppName("MessagePackCouchbaseSpark");
                setMaster("local");
                set("spark.couchbase.nodes", "172.17.0.3");
                set("spark.couchbase.bucket." + "default", "");
            }
        };
        jsc = new JavaSparkContext(sparkConf);
	}
	
	@Message
	public static class Person {

		public String name;
		public int age;

		public Person() {
		}

		public Person(String name, int age) {
			this.name = name;
			this.age = age;
		}
	}
	
	@Message
	public static class NewPerson {

		public String name;
		public int age;
		public Date date;

		public NewPerson() {
		}

		public NewPerson(String name, int age, Date date) {
			this.name = name;
			this.age = age;
			this.date = date;
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		List<URI> hosts = new ArrayList<>();
		hosts.add(new URI("http://172.17.0.3:8091/pools"));
		CouchbaseClient client = new CouchbaseClient(hosts, "default", null, "password");
		
		// set
		Person src = new Person("blueskyarea", 27);
		MessagePack msgPack = new MessagePack();
		byte[] buffer = msgPack.write(src);
		for (byte b : buffer) {
			System.out.print(Integer.toHexString(b & 0xFF) + " ");
		}

		new MessagePackCouchbaseSpark();
		//List<JsonDocument> list = new ArrayList<>();
		//list.add(JsonDocument.create("test-key2", JsonObject.create().put("id", 123)));
		
		/*List<byte[]> byteList = new ArrayList<>();
		byteList.add(buffer);
		JavaRDD<byte[]> byteRdd = jsc.parallelize(byteList);
		byteRdd.foreach(data -> {
			CouchbaseClient client2 = new CouchbaseClient(hosts, "default", null, "password");
			client2.set("test-key2", data);
		});*/
		
		List<SerializableDocument> list = new ArrayList<>();
		list.add(SerializableDocument.create("test-key2", buffer));
		//list.add(SerializableDocument.create("test-key2", 123));
		couchbaseDocumentRDD(jsc.parallelize(list)).saveToCouchbase(StoreMode.UPSERT);
		
		// get
		//Object object = client.get("test-key2");
		//for (byte b: (byte[]) object) {
		//	System.out.print(Integer.toHexString(b & 0xFF) + " ");
		//}
		Cluster cluster = CouchbaseCluster.create("172.17.0.3");
		Bucket bucket = cluster.openBucket();
		SerializableDocument found = bucket.get("test-key2", SerializableDocument.class);

		System.out.println();
		Person dest = msgPack.read((byte[]) found.content(), Person.class);
		System.out.println(dest.name);
		System.out.println(dest.age);
		
		NewPerson dest2 = msgPack.read((byte[]) found.content(), NewPerson.class);
		System.out.println(dest2.name);
		System.out.println(dest2.age);
		System.out.println(dest2.date);
		
		jsc.close();
	}
}
