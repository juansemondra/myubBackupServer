import org.zeromq.ZMQ;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class BackupServerApp {
    private static final int PUERTO_POSICIONES = 5561; // Puerto del backup para recibir posiciones
    private static final int PUERTO_SOLICITUDES = 5558; // Puerto del backup para recibir solicitudes
    private static final int PUERTO_HEALTH_CHECK = 5570; // Puerto de health check para recibir señales del servidor principal

    private Map<Integer, int[]> taxisPosiciones;
    private Map<Integer, Boolean> taxisOcupados;
    private Map<Integer, Integer> taxisServicios;
    private ZMQ.Context context;
    private boolean isPrimaryServerAlive = true; // Para monitorear el servidor primario

    public BackupServerApp() {
        taxisPosiciones = new HashMap<>();
        taxisOcupados = new HashMap<>();
        taxisServicios = new HashMap<>();
        this.context = ZMQ.context(1);
    }

    public void recibirPosiciones(String ip) {
        ZMQ.Socket responder = context.socket(ZMQ.REP);
        responder.bind("tcp://" + ip + ":" + PUERTO_POSICIONES);

        System.out.println("BackupServer escuchando posiciones de taxis en " + ip + ":" + PUERTO_POSICIONES + "...");

        while (isPrimaryServerAlive) {
            try {
                String mensaje = responder.recvStr(0);
                System.out.println("Posición recibida: " + mensaje);

                String[] partes = mensaje.split(" ");
                int taxiId = Integer.parseInt(partes[0].split(":")[1]);
                String[] coordenadas = partes[1].replace("Pos:(", "").replace(")", "").split(",");
                int posX = Integer.parseInt(coordenadas[0]);
                int posY = Integer.parseInt(coordenadas[1]);
                boolean isBusy = Boolean.parseBoolean(partes[2].split(":")[1]);

                taxisPosiciones.put(taxiId, new int[]{posX, posY});
                taxisOcupados.put(taxiId, isBusy);

                System.out.println("Taxi " + taxiId + " actualizó su posición a (" + posX + ", " + posY + ") y está ocupado: " + isBusy);

                if (!isBusy) {
                    responder.send(("Taxi " + taxiId + " asignado a servicio.").getBytes(ZMQ.CHARSET));
                } else {
                    responder.send(("Taxi " + taxiId + " está ocupado.").getBytes(ZMQ.CHARSET));
                }

            } catch (Exception e) {
                System.err.println("Error al procesar la posición del taxi: " + e.getMessage());
            }
        }

        responder.close();
    }

    public void recibirSolicitudes(String ip) {
        ZMQ.Socket responder = context.socket(ZMQ.REP);
        responder.bind("tcp://" + ip + ":" + PUERTO_SOLICITUDES);

        System.out.println("BackupServer escuchando solicitudes de usuarios en " + ip + ":" + PUERTO_SOLICITUDES + "...");

        while (isPrimaryServerAlive) {
            try {
                String solicitud = responder.recvStr(0);
                System.out.println("Solicitud recibida: " + solicitud);

                String[] partes = solicitud.split(" ");
                if (partes.length >= 6) {
                    String[] coordenadas = partes[5].split(",");
                    int usuarioX = Integer.parseInt(coordenadas[0]);
                    int usuarioY = Integer.parseInt(coordenadas[1]);

                    Integer taxiAsignado = encontrarTaxiMasCercano(new int[]{usuarioX, usuarioY});

                    if (taxiAsignado != null) {
                        taxisOcupados.put(taxiAsignado, true);

                        String respuesta = "Taxi asignado: Taxi " + taxiAsignado;
                        responder.send(respuesta.getBytes(ZMQ.CHARSET));

                        taxisServicios.put(taxiAsignado, taxisServicios.getOrDefault(taxiAsignado, 0) + 1);
                        System.out.println("Taxi " + taxiAsignado + " asignado al usuario en posición (" + usuarioX + ", " + usuarioY + ")");
                    } else {
                        responder.send("No hay taxis disponibles".getBytes(ZMQ.CHARSET));
                        System.out.println("No se encontraron taxis disponibles para el usuario en (" + usuarioX + ", " + usuarioY + ")");
                    }
                } else {
                    System.out.println("Formato de solicitud inesperado. Solicitud recibida: " + solicitud);
                    responder.send("Formato de solicitud no válido".getBytes(ZMQ.CHARSET));
                }
            } catch (Exception e) {
                System.err.println("Error al procesar la solicitud de usuario: " + e.getMessage());
            }
        }

        responder.close();
    }

    private Integer encontrarTaxiMasCercano(int[] usuarioPos) {
        Integer taxiCercano = null;
        int distanciaMinima = Integer.MAX_VALUE;

        for (Map.Entry<Integer, int[]> entry : taxisPosiciones.entrySet()) {
            int taxiId = entry.getKey();
            int[] taxiPos = entry.getValue();
            int distancia = Math.abs(taxiPos[0] - usuarioPos[0]) + Math.abs(taxiPos[1] - usuarioPos[1]);

            if (!taxisOcupados.getOrDefault(taxiId, true)) {
                if (distancia < distanciaMinima || (distancia == distanciaMinima && taxiId < taxiCercano)) {
                    distanciaMinima = distancia;
                    taxiCercano = taxiId;
                }
            }
        }

        return taxiCercano;
    }

    public void iniciarServidor(String ip) {
        Thread posicionThread = new Thread(() -> recibirPosiciones(ip));
        Thread solicitudThread = new Thread(() -> recibirSolicitudes(ip));
        Thread healthCheckThread = new Thread(() -> iniciarHealthCheck(ip));

        posicionThread.start();
        solicitudThread.start();
        healthCheckThread.start();
    }

    private void iniciarHealthCheck(String ip) {
        ZMQ.Socket healthCheckResponder = context.socket(ZMQ.REP);
        healthCheckResponder.bind("tcp://" + ip + ":" + PUERTO_HEALTH_CHECK);

        System.out.println("BackupServer escuchando mensajes de health check en " + ip + ":" + PUERTO_HEALTH_CHECK + "...");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                String mensaje = healthCheckResponder.recvStr(0);
                if (mensaje.equals("ping")) {
                    healthCheckResponder.send("pong".getBytes(ZMQ.CHARSET));
                }
            } catch (Exception e) {
                System.err.println("Error en health check: " + e.getMessage());
            }
        }

        healthCheckResponder.close();
    }

    public static void main(String[] args) {
        try {
            String ipBackup = InetAddress.getLocalHost().getHostAddress();
            System.out.println("Iniciando BackupServer en la IP: " + ipBackup);

            BackupServerApp backupServer = new BackupServerApp();
            backupServer.iniciarServidor(ipBackup);

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error al iniciar el BackupServer: " + e.getMessage());
        }
    }
}