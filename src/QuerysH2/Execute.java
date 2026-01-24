/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package QuerysH2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ANTONIO.CORIA
 */
 


public class Execute {
  private static final Logger LOGGER = Logger.getLogger(Execute.class.getName());
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

  public int EntidadInicio(Connection con){       
        int id=0;
        String sql="SELECT ID FROM ENTIDAD_INICIO";
         try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

             if(rs.next()) {
                    id=rs.getInt("ID");
            }else{
                 return 0;
             }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Actor_Trabajador()", ex);
        }
        return id;
    } 
    
  public String NombreEntidad(int ID){
     String NEntidad="";
     switch (ID) {
    case 1:
        NEntidad = "Aguascalientes";
        break;
    case 2:
        NEntidad = "Baja California";
        break;
    case 3:
        NEntidad = "Baja California Sur";
        break;
    case 4:
        NEntidad = "Campeche";
        break;
    case 5:
        NEntidad = "Coahuila de Zaragoza";
        break;
    case 6:
        NEntidad = "Colima";
        break;
    case 7:
        NEntidad = "Chiapas";
        break;
    case 8:
        NEntidad = "Chihuahua";
        break;
    case 9:
        NEntidad = "Ciudad de México";
        break;
    case 10:
        NEntidad = "Durango";
        break;
    case 11:
        NEntidad = "Guanajuato";
        break;
    case 12:
        NEntidad = "Guerrero";
        break;
    case 13:
        NEntidad = "Hidalgo";
        break;
    case 14:
        NEntidad = "Jalisco";
        break;
    case 15:
        NEntidad = "México";
        break;
    case 16:
        NEntidad = "Michoacán de Ocampo";
        break;
    case 17:
        NEntidad = "Morelos";
        break;
    case 18:
        NEntidad = "Nayarit";
        break;
    case 19:
        NEntidad = "Nuevo León";
        break;
    case 20:
        NEntidad = "Oaxaca";
        break;
    case 21:
        NEntidad = "Puebla";
        break;
    case 22:
        NEntidad = "Querétaro";
        break;
    case 23:
        NEntidad = "Quintana Roo";
        break;
    case 24:
        NEntidad = "San Luis Potosí";
        break;
    case 25:
        NEntidad = "Sinaloa";
        break;
    case 26:
        NEntidad = "Sonora";
        break;
    case 27:
        NEntidad = "Tabasco";
        break;
    case 28:
        NEntidad = "Tamaulipas";
        break;
    case 29:
        NEntidad = "Tlaxcala";
        break;
    case 30:
        NEntidad = "Veracruz de Ignacio de la Llave";
        break;
    case 31:
        NEntidad = "Yucatán";
        break;
    case 32:
        NEntidad = "Zacatecas";
        break;
    default:
        NEntidad = "Entidad no válida";
        break;
}
   return NEntidad; 
  }
  

}



