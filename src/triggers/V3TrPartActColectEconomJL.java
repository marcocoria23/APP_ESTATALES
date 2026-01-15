package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.SQLException;

public class V3TrPartActColectEconomJL implements Trigger {

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

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {

        // ===== Índices (0-based) según tu CREATE TABLE =====
        // 0  NOMBRE_ORGANO_JURIS
        // 1  CLAVE_ORGANO
        // 2  EXPEDIENTE_CLAVE
        // 3  ID_ACTOR
        final int iACTOR = 4;
        final int iDEFENSA_ACT = 5;
        final int iNOMBRE_SINDICATO = 6;
        final int iREG_ASOC_SINDICAL = 7;
        final int iTIPO_SINDICATO = 8;
        final int iOTRO_ESP_SINDICATO = 9;
        final int iORG_OBRERA = 10;
        final int iNOMBRE_ORG_OBRERA = 11;   // numérico según tu DDL
        final int iOTRO_ESP_OBRERA = 12;

        final int iTIPO = 17;
        final int iRFC_PATRON = 18;
        final int iRAZON_SOCIAL_EMPR = 19;
        final int iCALLE = 20;
        final int iN_EXT = 21;
        final int iN_INT = 22;
        final int iCOLONIA = 23;
        final int iCP = 24;
        final int iENTIDAD_NOMBRE_EMPR = 25;
        final int iENTIDAD_CLAVE_EMPR = 26;
        final int iMUNICIPIO_NOMBRE_EMPR = 27;
        final int iMUNICIPIO_CLAVE_EMPR = 28;

        // ===== Lógica del trigger =====

        // if :NEW.ACTOR IS NULL THEN 99
        setIfNull(newRow, iACTOR, 99);

        // if :NEW.DEFENSA_ACT IS NULL THEN 9
        setIfNull(newRow, iDEFENSA_ACT, 9);

        Integer actor = asInt(newRow[iACTOR]);

        // ---- Actor demandante (actor = 3) ----
        if (actor != null && actor == 3) {

            setIfNull(newRow, iNOMBRE_SINDICATO, "No Identificado");
            setIfNull(newRow, iREG_ASOC_SINDICAL, "No Identificado");
            setIfNull(newRow, iTIPO_SINDICATO, 9);

            Integer tipoSind = asInt(newRow[iTIPO_SINDICATO]);
            if (tipoSind != null && tipoSind == 6) {
                setIfNull(newRow, iOTRO_ESP_SINDICATO, "No Especifico");
            }

            setIfNull(newRow, iORG_OBRERA, 9);

            Integer orgObr = asInt(newRow[iORG_OBRERA]);
            if (orgObr != null && orgObr == 1) {

                setIfNull(newRow, iNOMBRE_ORG_OBRERA, 9);

                Integer nombreOrgObr = asInt(newRow[iNOMBRE_ORG_OBRERA]);
                if (nombreOrgObr != null && nombreOrgObr == 8) {
                    setIfNull(newRow, iOTRO_ESP_OBRERA, "No Especifico");
                }
            }
        }

        // ---- Actor demandado/patrón (actor = 2) ----
        if (actor != null && actor == 2) {

            setIfNull(newRow, iTIPO, 9);

            Integer tipo = asInt(newRow[iTIPO]);

            // tipo = 1 => RFC_PATRON obligatorio
            if (tipo != null && tipo == 1) {
                setIfNull(newRow, iRFC_PATRON, "No Identificado");
            }

            // tipo = 2 => datos empresa/domicilio obligatorios
            if (tipo != null && tipo == 2) {
                setIfNull(newRow, iRAZON_SOCIAL_EMPR, "No Identificado");
                setIfNull(newRow, iCALLE, "No Identificado");
                setIfNull(newRow, iN_EXT, "No Identificado");
                setIfNull(newRow, iN_INT, "No Identificado");
                setIfNull(newRow, iCOLONIA, "No Identificado");
                setIfNull(newRow, iCP, "No Identificado");

                setIfNull(newRow, iENTIDAD_NOMBRE_EMPR, "No Identificado");
                setIfNull(newRow, iENTIDAD_CLAVE_EMPR, 99);

                setIfNull(newRow, iMUNICIPIO_NOMBRE_EMPR, "No Identificado");
                setIfNull(newRow, iMUNICIPIO_CLAVE_EMPR, 99999);
            }
        }
    }

    @Override public void close() { }
    @Override public void remove() { }
}
