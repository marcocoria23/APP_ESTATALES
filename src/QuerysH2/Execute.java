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
    "V3_TR_ORDINARIOJL",
    "V3_TR_PART_ACT_ORDINARIOJL",
    "V3_TR_PART_DEM_ORDINARIOJL",
    "V3_TR_INDIVIDUALJL",
    "V3_TR_PART_ACT_INDIVIDUALJL",
    "V3_TR_PART_DEM_INDIVIDUALJL",
    "V3_TR_COLECTIVOJL",
    "V3_TR_PART_ACT_COLECTIVOJL",
    "V3_TR_PART_DEM_COLECTIVOJL",
    "V3_TR_COLECT_ECONOMJL",
    "V3_TR_PART_ACT_COLECT_ECONOMJL",
    "V3_TR_PART_DEM_COLECT_ECONOMJL",
    "V3_TR_HUELGAJL",
    "V3_TR_PART_ACT_HUELGAJL",
    "V3_TR_PART_DEM_HUELGAJL",
    "V3_TR_PREF_CREDITOJL",
    "V3_TR_TERCERIASJL",
    "V3_TR_PARAPROCESALJL",
    "V3_TR_EJECUCIONJL"
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