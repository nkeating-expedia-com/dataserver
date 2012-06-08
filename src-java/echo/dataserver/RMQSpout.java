package echo.dataserver;

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

import static clojure.lang.Util.sneakyThrow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;


public class RMQSpout extends BaseRichSpout {
	private final Integer TIMEOUT = 100;

	private String queue;
	private String host;
	private Integer port;

	private SpoutOutputCollector collector;

	private transient QueueingConsumer queueConsumer;

	private final static Logger logger = LoggerFactory.getLogger(RMQSpout.class);


	public RMQSpout(String queue, String host, Integer port) {
		this.queue = queue;
		this.host = host;
		this.port = port;

		logger.debug("RMQSpout started");
	}

	private byte[] receive() {
		try {
			QueueingConsumer.Delivery delivery = queueConsumer.nextDelivery(TIMEOUT);

			if (delivery != null) {
				return delivery.getBody();
			}
		} catch (Exception e) {
			Marker fatal = MarkerFactory.getMarker("FATAL");
			logger.error(fatal, "RMQSpout - rmq receive() exception");
			throw sneakyThrow(e);
		}

		return null;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		try {
			ConnectionFactory connectionFactory = new ConnectionFactory();
			connectionFactory.setHost(host);
			connectionFactory.setPort(port);

			Connection connection = connectionFactory.newConnection();

			Channel channel = connection.createChannel();
			channel.queueDeclare(queue, false, false, false, null);

			queueConsumer = new QueueingConsumer(channel);
			channel.basicConsume(queue, true, queueConsumer);
		} catch (Exception e) {
			Marker fatal = MarkerFactory.getMarker("FATAL");
			logger.debug(fatal, "RMQSpout - spout open() exception");
			throw sneakyThrow(e);
		}
	}

	@Override
	public void nextTuple() {
		byte[] data = receive();

		if (data != null) {
			collector.emit(new Values(data));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}
}
