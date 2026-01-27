package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class V3TrPartActOrdinarioJL implements Trigger {

    private static final String Sql_Error = "INSERT INTO ERRORES_INSERT "
            + "(TABLA_DESTINO, CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID, SQLSTATE, ERRORCODE, MENSAJE, REGISTRO_RAW) "
            + "VALUES (?,?,?,?,?,?,?,?)";

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
            String tableName, boolean before, int type) {
        // no-op
    }

    private Integer asInt(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Number) {
            return ((Number) v).intValue();
        }
        String s = v.toString().trim();
        if (s.isEmpty()) {
            return null;
        }
        return Integer.valueOf(s);
    }

    private String asString(Object v) {
        if (v == null) {
            return null;
        }
        String s = v.toString().trim();
        return (s.isEmpty() || "null".equalsIgnoreCase(s)) ? null : s;
    }

    private void setIfNull(Object[] newRow, int idx, Object value) {
        if (newRow[idx] == null) {
            newRow[idx] = value;
        }
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

            final int iNOMBRE_SINDICATO = 13;
            final int iREG_ASOC_SINDICAL = 14;
            final int iTIPO_SINDICATO = 15;
            final int iOTRO_ESP_SINDICATO = 16;
            final int iORG_OBRERA = 17;
            final int iNOMBRE_ORG_OBRERA = 18;
            final int iOTRO_ESP_OBRERA = 19;

            // ===== Defaults base =====
            // if (:New.DEFENSA_ACT IS NULL) then 9
            setIfNull(newRow, iDEFENSA_ACT, 9);

            // if :NEW.ACTOR IS NULL then 99
            setIfNull(newRow, iACTOR, 99);

            Integer actor = asInt(newRow[iACTOR]);

           if (actor != null && (actor == 2 || actor == 5 || actor == 6)) {
                String claveOrgano = asString(newRow[iCLAVE_ORGANO]);
                String expediente = asString(newRow[iEXPEDIENTE_CLAVE]);
                String Id_actor = asString(newRow[iID_ACTOR]);
                try (PreparedStatement pe = conn.prepareStatement(Sql_Error)) {
                    pe.setString(1, "V3_TR_PART_ACT_ORDINARIOJL");
                    pe.setString(2, claveOrgano);
                    pe.setString(3, expediente);
                    pe.setString(4, Id_actor);
                    pe.setString(5, "");
                    pe.setInt(6, 0);
                    pe.setString(7, "El campo 'Actor' solo puede tener el valor= 1.-Trabajador, 3.-Sindicato, 4.-Coalición de trabajadores o 7.-Otro ");
                    pe.setString(8, "");
                    pe.executeUpdate();
                } catch (SQLException ex) {
                    // Si hasta la tabla de errores falla, al menos lo imprimimos
                    System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
                }
            }

            // ===== Actor: trabajador (actor = 1) =====
            if (actor != null && actor == 1) {
                setIfNull(newRow, iSEXO, 9);
                setIfNull(newRow, iEDAD, 99);
                setIfNull(newRow, iOCUPACION, 999);

                setIfNull(newRow, iNSS, "No Identificado");
                setIfNull(newRow, iCURP, "No Identificado");
                setIfNull(newRow, iRFC_TRABAJADOR, "No Identificado");

                setIfNull(newRow, iJORNADA, 9);
            }

            // ===== Sindicato (actor = 3) =====
            if (actor != null && actor == 3) {

                setIfNull(newRow, iNOMBRE_SINDICATO, "No Identificado");
                setIfNull(newRow, iREG_ASOC_SINDICAL, "No Identificado");
                setIfNull(newRow, iTIPO_SINDICATO, 9);

                Integer tipoSind = asInt(newRow[iTIPO_SINDICATO]);
                if (tipoSind != null && tipoSind == 6) {
                    setIfNull(newRow, iOTRO_ESP_SINDICATO, "No Especifico");
                }

                setIfNull(newRow, iORG_OBRERA, 9);

                Integer orgObrera = asInt(newRow[iORG_OBRERA]);
                if (orgObrera != null && orgObrera == 1) {
                    setIfNull(newRow, iNOMBRE_ORG_OBRERA, 9);

                    Integer nombreOrgObrera = asInt(newRow[iNOMBRE_ORG_OBRERA]);
                    if (nombreOrgObrera != null && nombreOrgObrera == 8) {
                        setIfNull(newRow, iOTRO_ESP_OBRERA, "No Especifico");
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }

    }

    @Override
    public void close() {
    }

    @Override
    public void remove() {
    }
}
