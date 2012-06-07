package echo.dataserver;

import org.scribe.model.*;
import org.scribe.builder.api.DefaultApi10a;


public class EchoOAuthProvider extends DefaultApi10a {
	@Override
	public String getAccessTokenEndpoint() {
		return null;
	}

	@Override
	public String getAuthorizationUrl(Token requestToken) {
		return null;
	}

	@Override
	public String getRequestTokenEndpoint() {
		return null;
	}

}
