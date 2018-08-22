package com.blueskyarea;

import java.io.IOException;

import org.msgpack.MessagePack;
import org.msgpack.annotation.MessagePackMessage;

public class TryMessagePack {
	@MessagePackMessage
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

	public static void main(String[] args) throws IOException {
		Person src = new Person("mh", 25);
		byte[] buffer = MessagePack.pack(src);
		for (byte b : buffer) {
			System.out.print(Integer.toHexString(b & 0xFF) + " ");
		}
		System.out.println();

		Person dst = MessagePack.unpack(buffer, Person.class);
		System.out.println("name:" + dst.name);
		System.out.println("age:" + dst.age);
	}
}
