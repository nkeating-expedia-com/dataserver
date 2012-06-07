package echo.dataserver;

import org.scribe.model.*;
import org.scribe.builder.api.DefaultApi10a;
import org.scribe.exceptions.OAuthException;


public class EchoOAuthProvider extends DefaultApi10a {
	@Override
	public String getAccessTokenEndpoint() {
		throw new OAuthException("Echo API only supports 2-legged OAuth calls");
	}

	@Override
	public String getAuthorizationUrl(Token requestToken) {
		throw new OAuthException("Echo API only supports 2-legged OAuth calls");
	}

	@Override
	public String getRequestTokenEndpoint() {
		throw new OAuthException("Echo API only supports 2-legged OAuth calls");
	}

}
