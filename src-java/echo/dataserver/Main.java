package echo.dataserver;


public class Main {

	public static void main(String[] args) {
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><!-- vim: set ts=2 sts=2 sw=2 et: --><feed xml:lang=\"en-US\" xmlns=\"http://www.w3.org/2005/Atom\" xmlns:activity=\"http://activitystrea.ms/spec/1.0/\" xmlns:thr=\"http://purl.org/syndication/thread/1.0\" xmlns:media=\"http://purl.org/syndication/atommedia\">"
				+ "<id>http://js-kit.com/atom/example.com</id>"
				+ "<generator uri=\"http://js-kit.com/\">JS-Kit.com Syndication</generator>"
				+ "<title>submit_items</title>"
				+ "<link rel=\"alternate\" href=\"http://js-kit.com/html/example.com\" type=\"text/html\"/>"
				+ "<link rel=\"self\" href=\"http://js-kit.com/atom/example.com\" type=\"application/atom+xml\"/>"
				+ "<updated>2009-11-27T17:03:21+00:00</updated>"
				+ "<icon>http://js-kit.com/favicon.ico</icon>"
				+ "<logo>http://js-kit.com/images/site/logo8-js-kit.png</logo>"
				+ "<entry>"
				+ "<id>http://example.com/activities/add/article/1</id>"
				+ "<published>2009-11-27T17:01:55+00:00</published>"
				+ "<author>"
				+ "<name>Test User</name>"
				+ "<uri>http://example.com/</uri>"
				+ "<email>test@example.com</email>"
				+ "</author>"
				+ "<link rel=\"alternate\" type=\"text/html\" href=\"http://example.com/permalink/article/1\"/>"
				+ "<activity:verb>http://activitystrea.ms/schema/1.0/post</activity:verb>"
				+ "<activity:actor>"
				+ "<activity:object-type>http://activitystrea.ms/schema/1.0/person</activity:object-type>"
				+ "<id>http://js-kit.com/users/202cb962ac59075b964b07152d234b70</id>"
				+ "<title>Somebody</title>"
				+ "</activity:actor>"
				+ "<activity:object>"
				+ "<activity:object-type>http://activitystrea.ms/schema/1.0/article</activity:object-type>"
				+ "<title>Article 1 title</title>"
				+ "<content>This is a1111 conten122!!!12t of th222e test article 1</content>"
				+ "<id>http://example.com/dataserver/1</id>"
				+ "</activity:object>"
				+ "<activity:target>"
				+ "<id>http://example.com/dataserver</id>"
				+ "<title>example.com</title>"
				+ "</activity:target>"
				+ "</entry>"
				+ "</feed>";

		String OAUTH = "{\"xml\":" + xml + ", " +
						"\"submit-tokens\":{\"key\":\"test-1.js-kit.com\", " +
											"\"secret\":\"5eb609327578195a00f5f47a08a72ae9\", " +
											"\"endpoint\":\"http://api.prokopov.ul.js-kit.com/v1/submit\"}}";
		
		Sender s = new Sender("dataserver.submit", "localhost", 5672);
		//RMQSpout r = new RMQSpout("echoq", "localhost", 5672);

		try {
			Integer i = 0;
			while (i < Integer.MAX_VALUE) {
				s.send(OAUTH);
				//byte[] data = r.receive();
				//if (data != null) {
				//	System.out.println("Receiver - " + new String(data));
				//}
				Thread.sleep(5000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
