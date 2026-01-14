/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package triggers;

/**
 *
 * @author ANTONIO.CORIA
 */

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


public class CreaTriggers {

    public void crearTriggerAudiencias(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_AUDIENCIASJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_AUDIENCIASJL_BI " +
    "BEFORE INSERT ON V3_TR_AUDIENCIASJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrAudienciasJL\""
  );
        System.out.println("Se creo trigger");
}
    }
    
}
    
    

