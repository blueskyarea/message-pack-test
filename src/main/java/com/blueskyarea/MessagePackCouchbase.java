package com.blueskyarea;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.mock.Bucket;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.client.MockClient;

public class MessagePackCouchbase {
	protected final BucketConfiguration bucketConfiguration = new BucketConfiguration();
	protected MockClient mockClient;
	protected CouchbaseMock couchbaseMock;

	public MessagePackCouchbase() throws IOException, InterruptedException {
		bucketConfiguration.numNodes = 1;
		bucketConfiguration.numReplicas = 1;
		bucketConfiguration.numVBuckets = 1024;
		bucketConfiguration.name = "bucketName";
		bucketConfiguration.type = Bucket.BucketType.COUCHBASE;
		bucketConfiguration.password = "password";
		List<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
		configList.add(bucketConfiguration);
		couchbaseMock = new CouchbaseMock(8091, configList);
		couchbaseMock.start();
		couchbaseMock.waitForStartup();
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
		Person src = new Person("blueskyarea", 25);
		MessagePack msgPack = new MessagePack();
		byte[] buffer = msgPack.write(src);
		for (byte b : buffer) {
			System.out.print(Integer.toHexString(b & 0xFF) + " ");
		}
		
		MessagePackCouchbase mpc = new MessagePackCouchbase();
		List<URI> hosts = new ArrayList<>();
		hosts.add(new URI("http://localhost:8091/pools"));
		CouchbaseClient client = new CouchbaseClient(hosts, "bucketName", null, "password");
		
		client.set("test-key", buffer);
		
		Object object = client.get("test-key");
		for (byte b: (byte[]) object) {
			System.out.print(Integer.toHexString(b & 0xFF) + " ");
		}
		System.out.println();
		
		Person dest = msgPack.read((byte[]) object, Person.class);
		System.out.println(dest.name);
		System.out.println(dest.age);
		
		NewPerson dest2 = msgPack.read((byte[]) object, NewPerson.class);
		System.out.println(dest2.name);
		System.out.println(dest2.age);
		System.out.println(dest2.date);
	}
}
