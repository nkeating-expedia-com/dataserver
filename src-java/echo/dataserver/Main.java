package echo.dataserver;


public class Main {

	public static void main(String[] args) {
		Sender s = new Sender("echoq", "localhost", 5672);
		RMQSpout r = new RMQSpout("echoq", "localhost", 5672);

		try {
			s.send("Hello, world!");
			byte[] data = r.receive();
			if (data != null) {
				System.out.println("Receiver - " + new String(data));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
