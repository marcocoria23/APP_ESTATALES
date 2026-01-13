/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Conexion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
/**
 *
 * @author ANTONIO.CORIA
 */
public class ConexionH2 {
    
    private static final String URL ="jdbc:h2:file:./Database/Mybd;MODE=Oracle;DATABASE_TO_UPPER=false"; // Ruta relativa
    private static final String USER = "sa";
    private static final String PASSWORD = "";

    // Método para obtener la conexión
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
}
    
    

