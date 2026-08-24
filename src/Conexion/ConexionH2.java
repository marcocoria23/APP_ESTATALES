/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Conexion;

import java.io.File;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
/**
 *
 * @author ANTONIO.CORIA
 */
/*public class ConexionH2 {
    
    private static final String URL ="jdbc:h2:file:./Database/Mybd;MODE=Oracle;DATABASE_TO_UPPER=false;DB_CLOSE_ON_EXIT=TRUE;AUTO_SERVER=TRUE"; // Ruta relativa
    private static final String USER = "sa";
    private static final String PASSWORD = "AppRalabEstatales2026";

    // Método para obtener la conexión
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
}*/

public class ConexionH2 {

    private static final String USER = "sa";
    private static final String PASSWORD = "AppRalabEstatales2026";

    private static String getURL() {

        try {

            File jarFile = new File(
                ConexionH2.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI()
            );

            File appFolder;

            if (jarFile.isFile()) {
                appFolder = jarFile.getParentFile();
            } else {
                // Cuando ejecutas desde NetBeans
                appFolder = new File(System.getProperty("user.dir"));
            }

            File database = new File(
                appFolder,
                "Database/Mybd"
            );

            String ruta = database
                    .getAbsolutePath()
                    .replace("\\", "/");

            System.out.println("Base H2: " + ruta);

            return "jdbc:h2:file:" + ruta
                    + ";MODE=Oracle"
                    + ";DATABASE_TO_UPPER=false"
                    + ";DB_CLOSE_ON_EXIT=TRUE"
                    + ";AUTO_SERVER=TRUE";

        } catch (URISyntaxException e) {
            throw new RuntimeException(
                "No se pudo determinar la ruta de la aplicación",
                e
            );
        }
    }

    public static Connection getConnection() throws SQLException {

        return DriverManager.getConnection(
            getURL(),
            USER,
            PASSWORD
        );
    }
}
    
    

