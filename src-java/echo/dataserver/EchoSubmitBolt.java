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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


public class EchoSubmitBolt extends BaseRichBolt {
	private String xml;

	private String key;
	private String secret;
	private String endpoint;

	private OutputCollector collector;


	public EchoSubmitBolt() {
		System.out.println("ECHOBolt started");
	}

	private void parseJSON(String json) {
		JSONParser parser = new JSONParser();

		try {
			JSONObject data = (JSONObject)parser.parse(json);
			JSONObject oauth = (JSONObject)data.get("submit-tokens");
			
			xml = (String)data.get("xml");
			key = (String)oauth.get("key");
			secret = (String)oauth.get("secret");
			endpoint = (String)oauth.get("endpoint");
		} catch (org.json.simple.parser.ParseException e) {
			e.printStackTrace();
		}
	}

	private void submit() {
		Token token = new Token("", "");
		OAuthService service = new ServiceBuilder().apiKey(key).apiSecret(secret).build();

		OAuthRequest request = new OAuthRequest(Verb.POST, endpoint + "v1/submit");
		request.addBodyParameter("content", xml);

		service.signRequest(token, request);

		Response response = request.send();

		System.out.println("http code " + response.getCode() + " " + response.getBody());
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		parseJSON(new String((byte[])input.getValue(0)));
		submit();

		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}
}
