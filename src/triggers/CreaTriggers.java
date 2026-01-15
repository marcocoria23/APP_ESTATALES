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
 
     public void crearTriggerColectEconom(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_COLECT_ECONOMJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_COLECT_ECONOMJL_BI " +
    "BEFORE INSERT ON V3_TR_COLECT_ECONOMJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrColectEconomJL\""
  );
        System.out.println("Se creo trigger");
}
    }
     
     
      public void crearTriggerColectivo(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_COLECTIVOJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_COLECTIVOJL_BI " +
    "BEFORE INSERT ON V3_TR_COLECTIVOJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrColectivoJL\""
  );
        System.out.println("Se creo trigger");
}
    }
 
    public void crearTriggerControlExpediente(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_CONTROL_EXPEDIENTEJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_CONTROL_EXPEDIENTEJL_BI " +
    "BEFORE INSERT ON V3_TR_CONTROL_EXPEDIENTEJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrControlExpedienteJL\""
  );
        System.out.println("Se creo trigger");
}
    }
    
     public void crearTriggerEjecucion(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_EJECUCIONJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_EJECUCIONJL_BI " +
    "BEFORE INSERT ON V3_TR_EJECUCIONJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrEjecucionJL\""
  );
        System.out.println("Se creo trigger");
}
    }
 
   public void crearTriggerHuelga(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_HUELGAJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_HUELGAJL_BI " +
    "BEFORE INSERT ON V3_TR_HUELGAJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrHuelgaJL\""
  );
        System.out.println("Se creo trigger");
}
    }

 public void crearTriggerIndividual(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_INDIVIDUALJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_INDIVIDUALJL_BI " +
    "BEFORE INSERT ON V3_TR_INDIVIDUALJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrIndividualJL\""
  );
        System.out.println("Se creo trigger");
}
    }   
  
  public void crearTriggerOrdinario(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_ORDINARIOJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_ORDINARIOJL_BI " +
    "BEFORE INSERT ON V3_TR_ORDINARIOJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrOrdinarioJL\""
  );
        System.out.println("Se creo trigger");
}
    }
  
   public void crearTriggerParaprocesal(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PARAPROCESALJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PARAPROCESALJL_BI " +
    "BEFORE INSERT ON V3_TR_PARAPROCESALJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrParaprocesalJL\""
  );
        System.out.println("Se creo trigger");
}
    }
   
    public void crearTriggerPartActColectEconom(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_ACT_COLECT_ECONOMJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PART_ACT_COLECT_ECONOMJL_BI " +
    "BEFORE INSERT ON V3_TR_PART_ACT_COLECT_ECONOMJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrPartActColectEconomJL\""
  );
        System.out.println("Se creo trigger");
}
    }
    
     public void crearTriggerPartActColectivo(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_ACT_COLECTIVOJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PART_ACT_COLECTIVOJL_BI " +
    "BEFORE INSERT ON V3_TR_PART_ACT_COLECTIVOJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrPartActColectivoJL\""
  );
        System.out.println("Se creo trigger");
}
    }
     
      public void crearTriggerPartActHuelga(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_ACT_HUELGAJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PART_ACT_HUELGAJL_BI " +
    "BEFORE INSERT ON V3_TR_PART_ACT_HUELGAJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrPartActHuelgaJL\""
  );
        System.out.println("Se creo trigger");
}
    }
      
       public void crearTriggerPartActIndividual(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_ACT_INDIVIDUALJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PART_ACT_INDIVIDUALJL_BI " +
    "BEFORE INSERT ON V3_TR_PART_ACT_INDIVIDUALJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrPartActIndividualJL\""
  );
        System.out.println("Se creo trigger");
}
    }
       
        public void crearTriggerPartActOrdinario(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_ACT_ORDINARIOJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PART_ACT_ORDINARIOJL_BI " +
    "BEFORE INSERT ON V3_TR_PART_ACT_ORDINARIOJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrPartActOrdinarioJL\""
  );
        System.out.println("Se creo trigger");
}
    }
        
         public void crearTriggerPartDemColectEconom(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_DEM_COLECT_ECONOMJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PART_DEM_COLECT_ECONOMJL_BI " +
    "BEFORE INSERT ON V3_TR_PART_DEM_COLECT_ECONOMJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrPartDemColectEconomJL\""
  );
        System.out.println("Se creo trigger");
}
    }
         
          public void crearTriggerPartDemColectivo(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_DEM_COLECTIVOJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PART_DEM_COLECTIVOJL_BI " +
    "BEFORE INSERT ON V3_TR_PART_DEM_COLECTIVOJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrPartDemColectivoJL\""
  );
        System.out.println("Se creo trigger");
}
    }
          
           public void crearTriggerPartDemHuelga(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_DEM_HUELGAJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PART_DEM_HUELGAJL_BI " +
    "BEFORE INSERT ON V3_TR_PART_DEM_HUELGAJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrPartDemHuelgaJL\""
  );
        System.out.println("Se creo trigger");
}
    }
           
            public void crearTriggerPartDemIndividual(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_DEM_INDIVIDUALJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PART_DEM_INDIVIDUALJL_BI " +
    "BEFORE INSERT ON V3_TR_PART_DEM_INDIVIDUALJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrPartDemIndividualJL\""
  );
        System.out.println("Se creo trigger");
}
    }
            
      public void crearTriggerPartDemOrdinario(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_DEM_ORDINARIOJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PART_DEM_ORDINARIOJL_BI " +
    "BEFORE INSERT ON V3_TR_PART_DEM_ORDINARIOJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrPartDemOrdinarioJL\""
  );
        System.out.println("Se creo trigger");
}
    }
      
       public void crearTriggerPrefCredito(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_PREF_CREDITOJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_PREF_CREDITOJL_BI " +
    "BEFORE INSERT ON V3_TR_PREF_CREDITOJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrPreferenciaCreditoJL\""
  );
        System.out.println("Se creo trigger");
}
    }
       
        public void crearTriggerTercerias(Connection con) throws SQLException {
    try (
     Statement st = con.createStatement()) {
     st.execute("DROP TRIGGER IF EXISTS V3_TR_TERCERIASJL_BI");
     st.execute(
    "CREATE TRIGGER V3_TR_TERCERIASJL_BI " +
    "BEFORE INSERT ON V3_TR_TERCERIASJL " +
    "FOR EACH ROW " +
    "CALL \"triggers.V3TrTerceriasJL\""
  );
        System.out.println("Se creo trigger");
}
    }
     
}
    
    

