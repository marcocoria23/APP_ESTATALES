package Conexion;

import java.io.File;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConexionH2 {

    private static final String USER = "sa";
    private static final String PASSWORD = "AppRalabEstatales2026";

    /**
     * Obtiene la carpeta donde se encuentra la aplicación.
     */
    private static File getAppFolder() {

        try {

            File ubicacion = new File(
                    ConexionH2.class
                            .getProtectionDomain()
                            .getCodeSource()
                            .getLocation()
                            .toURI()
            );

            // Ejecutando desde el JAR
            if (ubicacion.isFile()) {
                return ubicacion.getParentFile();
            }

            // Ejecutando desde NetBeans
            return new File(System.getProperty("user.dir"));

        } catch (URISyntaxException e) {

            throw new RuntimeException(
                    "No se pudo determinar la ruta de la aplicación",
                    e
            );
        }
    }

    /**
     * Obtiene el archivo físico de la BD.
     */
    private static File getDatabaseFile() {

        return new File(
                getAppFolder(),
                "Database/RalabeEstatales.mv.db"
        );
    }

    /**
     * Construye la URL de conexión.
     */
    private static String getURL() throws SQLException {

        File archivoBD = getDatabaseFile();

        // =========================================
        // Crear carpeta Database si no existe
        // =========================================

        File carpetaBD = archivoBD.getParentFile();

        if (!carpetaBD.exists()) {

            boolean creada = carpetaBD.mkdirs();

            if (!creada && !carpetaBD.exists()) {
                throw new SQLException(
                        "No se pudo crear la carpeta:\n"
                        + carpetaBD.getAbsolutePath()
                );
            }

            System.out.println(
                    "Carpeta Database creada correctamente."
            );
        }

        // =========================================
        // Información de la BD
        // =========================================

        System.out.println("======================================");
        System.out.println("BASE DE DATOS H2");
        System.out.println(
                "Ruta: " + archivoBD.getAbsolutePath()
        );
        System.out.println(
                "Existe: " + archivoBD.exists()
        );

        if (archivoBD.exists()) {

            System.out.println(
                    "Tamaño: "
                    + archivoBD.length()
                    + " bytes"
            );

            System.out.println(
                    "Última modificación: "
                    + new java.util.Date(
                            archivoBD.lastModified()
                    )
            );

        } else {

            System.out.println(
                    "La BD no existe. H2 creará una nueva."
            );
        }

        System.out.println("======================================");

        // =========================================
        // Ruta SIN .mv.db
        // =========================================

        String ruta = archivoBD
                .getAbsolutePath()
                .replace("\\", "/");

        ruta = ruta.substring(
                0,
                ruta.length() - ".mv.db".length()
        );

        /*
         * IMPORTANTE:
         *
         * NO ponemos IFEXISTS=TRUE.
         *
         * De esta manera H2:
         *
         * - Si existe -> abre la BD.
         * - Si no existe -> crea una BD nueva.
         */

        return "jdbc:h2:file:"
                + ruta
                + ";MODE=Oracle"
                + ";DATABASE_TO_UPPER=false"
                + ";DB_CLOSE_ON_EXIT=TRUE";
    }

    /**
     * Obtiene una conexión a H2.
     */
    public static Connection getConnection()
            throws SQLException {

        String url = getURL();

        try {

            Connection con = DriverManager.getConnection(
                    url,
                    USER,
                    PASSWORD
            );

            System.out.println(
                    "Conexión H2 correcta."
            );

            System.out.println(
                    "Versión H2: "
                    + con.getMetaData()
                            .getDatabaseProductVersion()
            );

            return con;

        } catch (SQLException e) {

            System.err.println(
                    "ERROR AL CONECTAR CON H2"
            );

            System.err.println(
                    "URL: " + url
            );

            System.err.println(
                    "Usuario: " + USER
            );

            System.err.println(
                    "Código H2: "
                    + e.getErrorCode()
            );

            System.err.println(
                    "SQLState: "
                    + e.getSQLState()
            );

            System.err.println(
                    "Mensaje: "
                    + e.getMessage()
            );

            throw e;
        }
    }
}