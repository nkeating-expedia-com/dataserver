package ru.juise.submitter;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;


public class Receiver extends BaseRichSpout {
	private SpoutOutputCollector collector;

	private QueueingConsumer queueConsumer;

	public Receiver(String queue, String host, Integer port) throws Exception {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(host);
		connectionFactory.setPort(port);

		Connection connection = connectionFactory.newConnection();

		Channel channel = connection.createChannel();
		channel.queueDeclare(queue, false, false, false, null);

		QueueingConsumer queueConsumer = new QueueingConsumer(channel);
		channel.basicConsume(queue, true, queueConsumer);
	}

	public String receive() throws Exception {
		QueueingConsumer.Delivery delivery = queueConsumer.nextDelivery();

		System.out.println("Receiver - " + new String(delivery.getBody()));
		return new String(delivery.getBody());
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		try {
			collector.emit(new Values(receive()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}
}
