package ru.juise.submitter;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;


public class Sender {
	private String queue;

	private String host;
	private Integer port;

	public Sender(String queue, String host, Integer port) {
		this.queue = queue;
		this.host = host;
		this.port = port;
	}

	public void send(String msg) throws Exception {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(host);
		connectionFactory.setPort(port);

		Connection connection = connectionFactory.newConnection();

		Channel channel = connection.createChannel();
		channel.queueDeclare(queue, false, false, false, null);

		channel.basicPublish("", queue, null, msg.getBytes());
		System.out.println("Sender - Ok.");

		channel.close();
		connection.close();
	}
}
