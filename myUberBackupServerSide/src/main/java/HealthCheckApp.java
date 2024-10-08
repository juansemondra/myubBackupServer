import org.zeromq.ZMQ;

public class HealthCheckApp {
    private static final int PUERTO_HEALTH_CHECK = 5570;

    public static void main(String[] args) {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket healthCheckRequester = context.socket(ZMQ.REQ);
        healthCheckRequester.connect("tcp://127.0.0.1:" + PUERTO_HEALTH_CHECK);

        while (true) {
            try {
                healthCheckRequester.send("ping".getBytes(ZMQ.CHARSET));
                String respuesta = healthCheckRequester.recvStr(0);
                if (!respuesta.equals("pong")) {
                    System.err.println("Servidor principal no responde. Cambiando a servidor de respaldo.");
                    break;
                }
                Thread.sleep(5000);
            } catch (Exception e) {
                System.err.println("Error en el health check: " + e.getMessage());
                break;
            }
        }

        healthCheckRequester.close();
        context.close();
    }
}