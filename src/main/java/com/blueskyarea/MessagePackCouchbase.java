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

public class MessagePackCouchbase {
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
		Person src = new Person("blueskyarea", 27);
		MessagePack msgPack = new MessagePack();
		byte[] buffer = msgPack.write(src);
		for (byte b : buffer) {
			System.out.print(Integer.toHexString(b & 0xFF) + " ");
		}
		
		List<URI> hosts = new ArrayList<>();
		hosts.add(new URI("http://172.17.0.3:8091/pools"));
		CouchbaseClient client = new CouchbaseClient(hosts, "default", null, "password");
		
		//client.set("test-key", buffer);
		client.set("test-key", 123);
		Object object = client.get("test-key");
		System.out.println(object);
		System.out.println((int)object);
		
		/*Object object = client.get("test-key");
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
		System.out.println(dest2.date);*/
		
		//client.shutdown();
	}
}
