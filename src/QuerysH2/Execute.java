/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package QuerysH2;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author ANTONIO.CORIA
 */
public class Execute {
  String sql = "";
String[] Tabla = {
    "V3_TR_AUDIENCIASJL",
    "ERRORES_INSERT",
    "V3_TR_CONTROL_EXPEDIENTEJL",
    "V3_TR_ORDINARIOJL"
};

public void LimpiaTablas(Connection con) throws SQLException {
    try (Statement stmt = con.createStatement()) {
        for (String tabla : Tabla) {
            String sql = "TRUNCATE TABLE " + tabla;
            stmt.executeUpdate(sql);
            System.out.println("✅ Tabla " + tabla + " limpiada correctamente");
        }
    }
}





}