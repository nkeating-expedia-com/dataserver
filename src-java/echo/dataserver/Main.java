package echo.dataserver;


public class Main {

	public static void main(String[] args) {
		try {
			Sender s = new Sender("echoq", "localhost", 5672);
			RMQSpout r = new RMQSpout("echoq", "localhost", 5672);

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
