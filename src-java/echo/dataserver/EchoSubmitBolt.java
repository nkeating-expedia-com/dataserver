package echo.dataserver;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.scribe.builder.ServiceBuilder;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


public class EchoSubmitBolt extends BaseRichBolt {
	private OutputCollector collector;

	private final static Logger logger = LoggerFactory.getLogger(RMQSpout.class);


	public EchoSubmitBolt() {
		logger.debug("EchoSubmitBolt started");
	}

	private void submit(String key, String secret, String endpoint, String xml) {
		Token token = new Token("", "");
		OAuthService service = new ServiceBuilder().apiKey(key).apiSecret(secret).provider(EchoOAuthProvider.class).build();

		OAuthRequest request = new OAuthRequest(Verb.POST, endpoint);
		request.addBodyParameter("content", xml);

		service.signRequest(token, request);

		Response response = request.send();

		logger.info("EchoSubmitBolt - http code " + response.getCode() + " " + response.getBody());
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String xml;

		String key;
		String secret;
		String endpoint;

		JSONParser parser = new JSONParser();

		try {
			JSONObject data = (JSONObject)parser.parse(new String((byte[])input.getValue(0)));
			JSONObject oauth = (JSONObject)data.get("submit-tokens");

			xml = (String)data.get("xml");
			key = (String)oauth.get("key");
			secret = (String)oauth.get("secret");
			endpoint = (String)oauth.get("endpoint");

			submit(key, secret, endpoint, xml);
		} catch (org.json.simple.parser.ParseException e) {
			logger.error("EchoSubmitBolt - " + e.getMessage());
		} finally {
			collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}
}
