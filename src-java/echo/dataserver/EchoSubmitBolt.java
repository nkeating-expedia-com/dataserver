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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EchoSubmitBolt extends BaseRichBolt {
	private final int OK = 0;
	private final int ERR_WITH_RETRY = 1;
	private final int ERR_WITH_LOG = 2;

	private OutputCollector collector;

	private final static Logger logger = LoggerFactory.getLogger(EchoSubmitBolt.class);


	public EchoSubmitBolt() {
		logger.debug("started");
	}

	private int responseAnalyze(int httpCode, String httpMsg) {
		// OAuth 
		if (httpCode == 401) {
			JSONParser parser = new JSONParser();

			try {
				JSONObject data = (JSONObject)parser.parse(httpMsg);
				String errorCode = (String)data.get("errorCode");

				if (errorCode.equals("oauth_nonce_already_used")) return ERR_WITH_RETRY;
				else return ERR_WITH_LOG;
			} catch (org.json.simple.parser.ParseException e) {
				logger.error("HTTP reply JSON parser exception in position:" + e.getPosition() + "\n" + httpMsg, e);
			}
		}

		if (httpCode == 408 || httpCode == 429) return ERR_WITH_RETRY;
		if (httpCode == 501) return ERR_WITH_LOG;

		if (httpCode >= 200 && httpCode <= 299) return OK;
		if (httpCode >= 400 && httpCode <= 499) return ERR_WITH_LOG;
		if (httpCode >= 500 && httpCode <= 599) return ERR_WITH_RETRY;

		return OK;
	}

	private boolean submit(String key, String secret, String endpoint, String xml) {
		int result;

		Token token = new Token("", "");
		OAuthService service = new ServiceBuilder().apiKey(key).apiSecret(secret).provider(EchoOAuthProvider.class).build();

		OAuthRequest request = new OAuthRequest(Verb.POST, endpoint);
		request.addBodyParameter("content", xml);

		service.signRequest(token, request);
		Response response = request.send();

		result = responseAnalyze(response.getCode(), response.getBody());

		if (result == OK) {
			logger.debug("HTTP result code:" + response.getCode() + ", reply message:" + response.getBody());
			return true;
		} else if (result == ERR_WITH_RETRY) {
			logger.info("HTTP result code:" + response.getCode() + ", reply message:" + response.getBody());
			return false;
		} else if (result == ERR_WITH_LOG) {
			logger.error("HTTP result code:" + response.getCode() + ", reply message:" + response.getBody());
			logger.error("Content data error:\nkey:" + key + "\nendpoint:" + endpoint + "\nxml:" + xml);
		}

		return true;
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

		boolean result = true;

		JSONParser parser = new JSONParser();

		try {
			JSONObject data = (JSONObject)parser.parse(new String((byte[])input.getValue(0)));
			JSONObject oauth = (JSONObject)data.get("submit-tokens");

			xml = (String)data.get("xml");
			key = (String)oauth.get("key");
			secret = (String)oauth.get("secret");
			endpoint = (String)oauth.get("endpoint");

			result = submit(key, secret, endpoint, xml);
		} catch (org.json.simple.parser.ParseException e) {
			logger.error("JSON parser exception in position:" + e.getPosition() + "\n" + new String((byte[])input.getValue(0)), e);
		} finally {
			if (result) {
				collector.ack(input);
			} else {
				collector.fail(input);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}
}
