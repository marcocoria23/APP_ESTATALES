package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.SQLException;

public class V3TrPartActHuelgaJL implements Trigger {

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
        final int iNOMBRE_ORGANO_JURIS   = 0;
        final int iCLAVE_ORGANO          = 1;
        final int iEXPEDIENTE_CLAVE      = 2;
        final int iID_ACTOR              = 3;

        final int iACTOR                 = 4;
        final int iDEFENSA_ACT           = 5;

        final int iNOMBRE_SINDICATO      = 6;
        final int iREG_ASOC_SINDICAL     = 7;
        final int iTIPO_SINDICATO        = 8;
        final int iOTRO_ESP_SINDICATO    = 9;

        final int iORG_OBRERA            = 10;
        final int iNOMBRE_ORG_OBRERA     = 11;
        final int iOTRO_ESP_OBRERA       = 12;

        // 13..17 cantidades; 18 comentarios

        // ===== Defaults generales =====
        setIfNull(newRow, iACTOR, 99);
        setIfNull(newRow, iDEFENSA_ACT, 9);

        Integer actor = asInt(newRow[iACTOR]);
        Integer tipoSind = asInt(newRow[iTIPO_SINDICATO]);
        Integer orgObrera = asInt(newRow[iORG_OBRERA]);
        Integer nombreOrgObrera = asInt(newRow[iNOMBRE_ORG_OBRERA]);

        // ===== Reglas para Actor = 3 (sindicato) =====
        if (actor != null && actor == 3) {
            setIfNull(newRow, iNOMBRE_SINDICATO, "No Especifico");
            setIfNull(newRow, iREG_ASOC_SINDICAL, "No Especifico");
            setIfNull(newRow, iTIPO_SINDICATO, 9);
            setIfNull(newRow, iORG_OBRERA, 9);

            // Recalcular por si se llenó con default
            tipoSind = asInt(newRow[iTIPO_SINDICATO]);
            orgObrera = asInt(newRow[iORG_OBRERA]);

            // Si tipo_sindicato = 6 -> otro_esp_sindicato obligatorio
            if (tipoSind != null && tipoSind == 6) {
                setIfNull(newRow, iOTRO_ESP_SINDICATO, "No Especifico");
            }

            // Si org_obrera = 1 -> nombre_org_obrera obligatorio
            if (orgObrera != null && orgObrera == 1) {
                setIfNull(newRow, iNOMBRE_ORG_OBRERA, 9);

                // Si nombre_org_obrera = 8 -> otro_esp_obrera obligatorio
                nombreOrgObrera = asInt(newRow[iNOMBRE_ORG_OBRERA]);
                if (nombreOrgObrera != null && nombreOrgObrera == 8) {
                    setIfNull(newRow, iOTRO_ESP_OBRERA, "No Especifico");
                }
            }
        } else {
            // Aun si NO es actor=3, tu Oracle tiene reglas sueltas:
            // if tipo_sindicato = 6 and otro_esp_sindicato is null -> 'No Especifico'
            if (tipoSind != null && tipoSind == 6) {
                setIfNull(newRow, iOTRO_ESP_SINDICATO, "No Especifico");
            }

            // if nombre_org_obrera = 8 and otro_esp_obrera is null -> 'No Especifico'
            if (nombreOrgObrera != null && nombreOrgObrera == 8) {
                setIfNull(newRow, iOTRO_ESP_OBRERA, "No Especifico");
            }

            // if org_obrera = 1 and nombre_org_obrera is null -> 9 (en Oracle no estaba condicionado a actor aquí)
            if (orgObrera != null && orgObrera == 1) {
                setIfNull(newRow, iNOMBRE_ORG_OBRERA, 9);
            }
        }
    }

    @Override public void close() { }
    @Override public void remove() { }
}
