package echo.dataserver;


public class Main {

	public static void main(String[] args) {
		Sender s = new Sender("dataserver.submit", "localhost", 5672);
		//RMQSpout r = new RMQSpout("echoq", "localhost", 5672);

		try {
			Integer i = 0;
			while (i < Integer.MAX_VALUE) {
				s.send(i++ + " Hello, world!");
				//byte[] data = r.receive();
				//if (data != null) {
				//	System.out.println("Receiver - " + new String(data));
				//}
				Thread.sleep(500);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
