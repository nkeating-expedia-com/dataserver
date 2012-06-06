package ru.juise.submitter;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;


public class Receiver {
	private String queue;

	private String host;
	private Integer port;

	public Receiver(String queue, String host, Integer port) {
		this.queue = queue;
		this.host = host;
		this.port = port;
	}

	public void receive() throws Exception {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(host);
		connectionFactory.setPort(port);

		Connection connection = connectionFactory.newConnection();

		Channel channel = connection.createChannel();
		channel.queueDeclare(queue, false, false, false, null);

		QueueingConsumer queueConsumer = new QueueingConsumer(channel);
		channel.basicConsume(queue, true, queueConsumer);

		QueueingConsumer.Delivery delivery;
		while (true) {
			delivery = queueConsumer.nextDelivery();

			System.out.println("Receiver - " + new String(delivery.getBody()));
		}
	}
}
