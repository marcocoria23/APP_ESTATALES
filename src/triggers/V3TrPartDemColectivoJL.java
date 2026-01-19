package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.SQLException;

public class V3TrPartDemColectivoJL implements Trigger {

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
try {
        // ===== Índices (0-based) según tu DDL =====
        // 0  NOMBRE_ORGANO_JURIS
        // 1  CLAVE_ORGANO
        // 2  EXPEDIENTE_CLAVE
        // 3  ID_DEMANDADO
        final int iDEMANDADO = 4;
        final int iDEFENSA_DEM = 5;

        final int iNOMBRE_SINDICATO_DEM = 6;
        final int iREG_ASOC_SINDICAL_DEM = 7;
        final int iTIPO_SINDICATO_DEM = 8;
        final int iOTRO_ESP_SINDICATO_DEM = 9;

        final int iORG_OBRERA_DEM = 10;
        final int iNOMBRE_ORG_OBRERA_DEM = 11;
        final int iOTRO_ESP_OBRERA_DEM = 12;

        // 13..17 cantidades (no se tocan en el trigger)
        final int iTIPO_DEM_PAT = 18;
        final int iRFC_PATRON_DEM = 19;
        final int iRAZON_SOCIAL_EMPR_DEM = 20;

        final int iCALLE = 21;
        final int iN_EXT = 22;
        final int iN_INT = 23;
        final int iCOLONIA = 24;
        final int iCP = 25;

        final int iENTIDAD_NOMBRE_EMPR = 26;
        final int iENTIDAD_CLAVE_EMPR = 27;
        final int iMUNICIPIO_NOMBRE_EMPR = 28;
        final int iMUNICIPIO_CLAVE_EMPR = 29;

        final int iLATITUD_EMPR = 30;
        final int iLONGITUD_EMPR = 31;

        // ===== Defaults base =====
        // if :New.DEMANDADO IS NULL then 9
        setIfNull(newRow, iDEMANDADO, 9);

        // if :New.DEFENSA_DEM IS NULL then 9
        setIfNull(newRow, iDEFENSA_DEM, 9);

        Integer demandado = asInt(newRow[iDEMANDADO]);

        // ===== Demandado sindicato (demandado = 2) =====
        if (demandado != null && demandado == 2) {

            setIfNull(newRow, iNOMBRE_SINDICATO_DEM, "No Identificado");
            setIfNull(newRow, iREG_ASOC_SINDICAL_DEM, "No Identificado");
            setIfNull(newRow, iTIPO_SINDICATO_DEM, 9);

            Integer tipoSind = asInt(newRow[iTIPO_SINDICATO_DEM]);
            if (tipoSind != null && tipoSind == 6) {
                setIfNull(newRow, iOTRO_ESP_SINDICATO_DEM, "No Especifico");
            }

            setIfNull(newRow, iORG_OBRERA_DEM, 9);

            Integer orgObrera = asInt(newRow[iORG_OBRERA_DEM]);
            if (orgObrera != null && orgObrera == 1) {
                setIfNull(newRow, iNOMBRE_ORG_OBRERA_DEM, 9);

                Integer nombreOrg = asInt(newRow[iNOMBRE_ORG_OBRERA_DEM]);
                if (nombreOrg != null && nombreOrg == 8) {
                    // Oracle: 'No especifico' (minúscula). Lo dejo igual.
                    setIfNull(newRow, iOTRO_ESP_OBRERA_DEM, "No especifico");
                }
            }
        }

        // ===== Demandado patrón (demandado = 1) =====
        if (demandado != null && demandado == 1) {

            setIfNull(newRow, iTIPO_DEM_PAT, 9);

            Integer tipoPat = asInt(newRow[iTIPO_DEM_PAT]);

            if (tipoPat != null && tipoPat == 1) {
                setIfNull(newRow, iRFC_PATRON_DEM, "No Identificado");
            }

            if (tipoPat != null && tipoPat == 2) {
                setIfNull(newRow, iRAZON_SOCIAL_EMPR_DEM, "No Identificado");
                setIfNull(newRow, iCALLE, "No Identificado");
                setIfNull(newRow, iN_EXT, "No Identificado");
                setIfNull(newRow, iN_INT, "No Identificado");
                setIfNull(newRow, iCOLONIA, "No Identificado");
                setIfNull(newRow, iCP, "No Identificado");

                setIfNull(newRow, iENTIDAD_NOMBRE_EMPR, "No Identificado");
                setIfNull(newRow, iENTIDAD_CLAVE_EMPR, 99);

                setIfNull(newRow, iMUNICIPIO_NOMBRE_EMPR, "No Identificado");
                setIfNull(newRow, iMUNICIPIO_CLAVE_EMPR, 99999);

                setIfNull(newRow, iLATITUD_EMPR, "No Identificado");
                setIfNull(newRow, iLONGITUD_EMPR, "No Identificado");
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
