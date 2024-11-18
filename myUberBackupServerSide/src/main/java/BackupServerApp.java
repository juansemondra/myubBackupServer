import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class BackupServerApp {
    private static final int PUERTO_POSICIONES = 5560;
    private static final int PUERTO_SOLICITUDES = 5557;
    private static final int PUERTO_HEALTH_CHECK = 5570;
    private static final int PUERTO_TAXI_PUSH = 5561;
    private static final String PRIMARY_SERVER_IP = "127.0.0.1";

    private Map<Integer, int[]> taxisPosiciones;
    private Map<Integer, Boolean> taxisOcupados;
    private Map<Integer, Integer> taxisServicios;
    private ZMQ.Context context;
    private boolean isPrimaryServerAlive = true;

    public BackupServerApp() {
        taxisPosiciones = new HashMap<>();
        taxisOcupados = new HashMap<>();
        taxisServicios = new HashMap<>();
        this.context = ZMQ.context(1);
    }

    public void iniciarServidorHealthCheck(String ip) {
        Thread healthCheckThread = new Thread(() -> {
            try {
                iniciarHealthCheck(ip);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        });
        healthCheckThread.start();
    }

    private void iniciarHealthCheck(String ip) throws UnknownHostException {
        ZMQ.Socket healthCheckSocket = context.socket(ZMQ.REQ);
        healthCheckSocket.connect("tcp://" + PRIMARY_SERVER_IP + ":" + PUERTO_HEALTH_CHECK);

        System.out.println("HealthCheck iniciado.");

        long lastResponseTime = System.currentTimeMillis();
        final int TIMEOUT = 15000;

        ZMQ.Poller poller = context.poller(1);
        poller.register(healthCheckSocket, ZMQ.Poller.POLLIN);

        while (true) {
            try {
                healthCheckSocket.send("PING".getBytes(ZMQ.CHARSET));
                int events = poller.poll(5000);
                if (events > 0 && poller.pollin(0)) {
                    String respuesta = healthCheckSocket.recvStr(0);
                    if ("PONG".equals(respuesta)) {
                        lastResponseTime = System.currentTimeMillis();
                        System.out.println("El Healthcheck recibe respuesta del sevidor principal\n.");
                        if (!isPrimaryServerAlive) {
                            System.out.println("Servidor principal ha vuelto a estar activo.\n");
                            isPrimaryServerAlive = true;
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error en HealthCheck: " + e.getMessage());
            }

            if (System.currentTimeMillis() - lastResponseTime > TIMEOUT) {
                if (isPrimaryServerAlive) {
                    isPrimaryServerAlive = false;
                    System.err.println("Servidor principal no responde durante 15 segundos. Activando servidor de respaldo...");

                    String ipLocal = InetAddress.getLocalHost().getHostAddress();
                    BackupServerApp servidor_backup = new BackupServerApp();
                    servidor_backup.iniciarServidorBackup(ipLocal);

                }
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        healthCheckSocket.close();
    }

    public void iniciarServidorBackup(String ip) {
        ZMQ.Socket posicionesSocket = context.socket(ZMQ.REP);
        posicionesSocket.bind("tcp://" + ip + ":" + PUERTO_POSICIONES);

        ZMQ.Socket solicitudesSocket = context.socket(ZMQ.REP);
        solicitudesSocket.bind("tcp://" + ip + ":" + PUERTO_SOLICITUDES);

        ZMQ.Socket taxiPushSocket = context.socket(ZMQ.PUSH);
        taxiPushSocket.bind("tcp://" + ip + ":" + PUERTO_TAXI_PUSH);

        System.out.println("Servidor iniciado con éxito en " + ip + ":" + PUERTO_POSICIONES + ", " + PUERTO_SOLICITUDES);

        Poller poller = context.poller(3);
        poller.register(posicionesSocket, Poller.POLLIN);
        poller.register(solicitudesSocket, Poller.POLLIN);

        while (!Thread.currentThread().isInterrupted()) {
            poller.poll();

            if (poller.pollin(0)) {
                procesarPosiciones(posicionesSocket);
            }
            if (poller.pollin(1)) {
                procesarSolicitudes(solicitudesSocket, taxiPushSocket);
            }
        }

        posicionesSocket.close();
        solicitudesSocket.close();
        taxiPushSocket.close();
        context.close();
    }

    private int calcularDistancia(int[] pos1, int[] pos2) {
        return Math.abs(pos1[0] - pos2[0]) + Math.abs(pos1[1] - pos2[1]);
    }

    private void procesarPosiciones(ZMQ.Socket socket) {
        try {
            String mensaje = socket.recvStr(0);
            System.out.println("\nActualización de taxi recibida: " + mensaje);

            String[] partes = mensaje.split(" ");
            if (partes.length >= 3 && partes[0].startsWith("TaxiID:") && partes[1].startsWith("Pos:")) {
                int taxiId = Integer.parseInt(partes[0].split(":")[1]);
                String[] coordenadas = partes[1].replace("Pos:(", "").replace(")", "").split(",");
                if (coordenadas.length == 2) {
                    int posX = Integer.parseInt(coordenadas[0]);
                    int posY = Integer.parseInt(coordenadas[1]);
                    boolean isBusy = Boolean.parseBoolean(partes[2].split(":")[1]);

                    taxisPosiciones.put(taxiId, new int[]{posX, posY});
                    taxisOcupados.put(taxiId, isBusy);

                    actualizarArchivoTaxi(taxiId, "Posición: (" + posX + "," + posY + "), Busy: " + isBusy);

                    socket.send(("Taxi " + taxiId + " no hacer nada.").getBytes(ZMQ.CHARSET));
                    System.out.println("Taxi " + taxiId + " actualizó su posición a (" + posX + ", " + posY + ") y está ocupado: " + isBusy);
                } else {
                    System.err.println("Error: coordenadas inválidas en el mensaje del taxi.");
                }
            } else {
                System.err.println("Error: formato inválido del mensaje del taxi.");
            }
        } catch (Exception e) {
            System.err.println("Error al procesar la posición del taxi: " + e.getMessage());
        }
    }

    private void procesarSolicitudes(ZMQ.Socket socket, ZMQ.Socket taxiPushSocket) {
        try {
            String solicitud = socket.recvStr(0);
            System.out.println("\nProcesando solicitud de usuario: " + solicitud);
            String[] partes = solicitud.split(" ");
            if (partes.length >= 6) {
                String[] coordenadas = partes[5].split(",");
                int usuarioX = Integer.parseInt(coordenadas[0]);
                int usuarioY = Integer.parseInt(coordenadas[1]);

                Integer taxiAsignado = encontrarTaxiMasCercano(new int[]{usuarioX, usuarioY});

                if (taxiAsignado != null) {
                    taxisOcupados.put(taxiAsignado, true);
                    actualizarArchivoTaxi(taxiAsignado, "Solicitud usuario: (" + usuarioX + "," + usuarioY + ")");

                    actualizarEstadisticasTaxi(taxiAsignado, true);

                    String respuesta = "Taxi asignado: Taxi " + taxiAsignado;
                    socket.send(respuesta.getBytes(ZMQ.CHARSET));

                    taxiPushSocket.send(("TaxiID:" + taxiAsignado + " ha sido asignado").getBytes(ZMQ.CHARSET));
                    System.out.println("TaxiID:" + taxiAsignado + " ha sido asignado");
                } else {
                    socket.send("No hay taxis disponibles".getBytes(ZMQ.CHARSET));
                }
            } else {
                socket.send("Formato de solicitud no válido".getBytes(ZMQ.CHARSET));
            }
        } catch (Exception e) {
            System.err.println("Error al procesar la solicitud de usuario: " + e.getMessage());
        }
    }

    private Integer encontrarTaxiMasCercano(int[] usuarioPos) {
        Integer taxiCercano = null;
        int distanciaMinima = Integer.MAX_VALUE;

        for (Map.Entry<Integer, int[]> entry : taxisPosiciones.entrySet()) {
            int taxiId = entry.getKey();
            int[] taxiPos = entry.getValue();
            int distancia = calcularDistancia(taxiPos, usuarioPos);

            if (!taxisOcupados.getOrDefault(taxiId, true)) {
                if (distancia < distanciaMinima || (distancia == distanciaMinima && taxiId < taxiCercano)) {
                    distanciaMinima = distancia;
                    taxiCercano = taxiId;
                }
            }
        }

        return taxiCercano;
    }

    private void verificarArchivoTaxi(int taxiId) throws IOException {
        String fileName = taxiId + ".txt";
        File file = new File(fileName);
        if (!file.exists()) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                writer.write("ID-" + taxiId);
                writer.newLine();
                writer.write("Cantidad_Servicios-0");
                writer.newLine();
                writer.write("Servicios_Aceptados-0");
                writer.newLine();
                writer.write("Servicios_Rechazados-0");
                writer.newLine();
            }
        }
    }

    private void actualizarArchivoTaxi(int taxiId, String linea) throws IOException {
        verificarArchivoTaxi(taxiId);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(taxiId + ".txt", true))) {
            writer.write(linea);
            writer.newLine();
        }
    }

    private void actualizarEstadisticasTaxi(int taxiId, boolean aceptado) throws IOException {
        verificarArchivoTaxi(taxiId);

        File file = new File(taxiId + ".txt");
        StringBuilder content = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("Cantidad_Servicios")) {
                    int cantidad = Integer.parseInt(line.split("-")[1]) + 1;
                    content.append("Cantidad_Servicios-").append(cantidad).append("\n");
                } else if (line.startsWith("Servicios_Aceptados") && aceptado) {
                    int aceptados = Integer.parseInt(line.split("-")[1]) + 1;
                    content.append("Servicios_Aceptados-").append(aceptados).append("\n");
                } else if (line.startsWith("Servicios_Rechazados") && !aceptado) {
                    int rechazados = Integer.parseInt(line.split("-")[1]) + 1;
                    content.append("Servicios_Rechazados-").append(rechazados).append("\n");
                } else {
                    content.append(line).append("\n");
                }
            }
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(content.toString());
        }
    }

    public static void main(String[] args) {
        try {
            String ipBackup = InetAddress.getLocalHost().getHostAddress();
            BackupServerApp backupServer = new BackupServerApp();
            backupServer.iniciarServidorHealthCheck(ipBackup);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}