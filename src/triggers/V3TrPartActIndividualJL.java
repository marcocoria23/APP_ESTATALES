package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class V3TrPartActIndividualJL implements Trigger {
    
     private static final String Sql_Error="INSERT INTO ERRORES_INSERT "
            + "(TABLA_DESTINO, CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID, "
            + " SQLSTATE, ERRORCODE, MENSAJE, REGISTRO_RAW) "
            + "VALUES (?,?,?,?,?,?,?,?)";

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
                     String tableName, boolean before, int type) {
        // no-op
    }

    private Integer asInt(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).intValue();
        String s = v.toString().trim();
        if (s.isEmpty()) return null;
        return Integer.valueOf(s);
    }

    private void setIfNull(Object[] newRow, int idx, Object value) {
        if (newRow[idx] == null) newRow[idx] = value;
    }
    
        private String asString(Object v) {
if (v == null) return null;
String s = v.toString().trim();
return (s.isEmpty() || "null".equalsIgnoreCase(s)) ? null : s;
}
    

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
try {

        // ===== Índices (0-based) según tu DDL =====
        // 0  NOMBRE_ORGANO_JURIS
        // 1  CLAVE_ORGANO
        // 2  EXPEDIENTE_CLAVE
        // 3  ID_ACTOR
        final int iCLAVE_ORGANO = 1;
        final int iEXPEDIENTE_CLAVE = 2;
        final int iID_ACTOR = 3;
        final int iACTOR = 4;
        final int iDEFENSA_ACT = 5;
        final int iSEXO = 6;
        final int iEDAD = 7;
        final int iOCUPACION = 8;
        final int iNSS = 9;
        final int iCURP = 10;
        final int iRFC_TRABAJADOR = 11;
        final int iJORNADA = 12;

        // ===== Defaults base =====
        // if :NEW.ACTOR IS NULL THEN 99
        setIfNull(newRow, iACTOR, 99);

        // if :NEW.DEFENSA_ACT IS NULL THEN 9
        setIfNull(newRow, iDEFENSA_ACT, 9);

        Integer actor = asInt(newRow[iACTOR]);

        // ===== Actor: trabajador (actor = 1) =====
        if (actor != null && actor == 1) {

            setIfNull(newRow, iSEXO, 9);
            setIfNull(newRow, iEDAD, 99);
            setIfNull(newRow, iOCUPACION, 999);

            setIfNull(newRow, iNSS, "No Identificado");
            setIfNull(newRow, iCURP, "No Identificado");
            setIfNull(newRow, iRFC_TRABAJADOR, "No Identificado");

            setIfNull(newRow, iJORNADA, 9);
            
            
             if (actor != null && (actor == 2 || actor == 3 || actor == 4 || actor == 5)) {
	String claveOrgano = asString(newRow[iCLAVE_ORGANO]);
        String expediente = asString(newRow[iEXPEDIENTE_CLAVE]);
        String idactor = asString(newRow[iID_ACTOR]);
          try ( PreparedStatement pe = conn.prepareStatement(Sql_Error)) {
            pe.setString(1, "V3_TR_PART_ACT_INDIVIDUALJL");
            pe.setString(2, claveOrgano);
            pe.setString(3, expediente);
            pe.setString(4, idactor);
            pe.setString(5, "");
            pe.setInt(6, 999);
            pe.setString(7, "El campo Actor solo puede tener el valor= 1.-Trabajador,6.-Beneficiario,7.-Otro");
            pe.setString(8, "");
            pe.executeUpdate();	
	} catch (SQLException ex) {
            // Si hasta la tabla de errores falla, al menos lo imprimimos
            System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
        }
          
            }
            
            
            
        }
     } catch (Exception e) {
            System.out.println("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }

}

    @Override public void close() { }
    @Override public void remove() { }
}
