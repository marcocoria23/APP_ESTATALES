package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;

public class V3TrHuelgaJL implements Trigger {

    private static final Date D_1899 = Date.valueOf("1899-09-09");
    private static final Date D_1999 = Date.valueOf("1999-09-09");

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

    private Date asDate(Object v) {
        if (v == null) return null;
        if (v instanceof Date) return (Date) v;
        if (v instanceof java.util.Date) return new Date(((java.util.Date) v).getTime());
        String s = v.toString().trim();
        if (s.isEmpty()) return null;

        // Si te llegara "09/09/1999" desde CSV, esto lo evitaría (opcional):
        if (s.equals("09/09/1999")) return D_1999;
        if (s.equals("09/09/1899")) return D_1899;

        // Formato esperado: yyyy-mm-dd
        return Date.valueOf(s);
    }

    private void setIfNull(Object[] newRow, int idx, Object value) {
        if (newRow[idx] == null) newRow[idx] = value;
    }

    private void replace1999With1899(Object[] newRow, int idxDate) {
        Date d = asDate(newRow[idxDate]);
        if (d != null && d.equals(D_1999)) newRow[idxDate] = D_1899;
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {

        // ===== Índices (0-based) según tu CREATE TABLE =====
        final int iFECHA_APERTURA_EXPEDIENTE = 3;
        final int iTIPO_ASUNTO              = 4;
        final int iRAMA_INDUS_INVOLUCRAD    = 5;
        final int iSECTOR_RAMA              = 6;
        final int iSUBSECTOR_RAMA           = 7;

        final int iFIRMA_CONTRATO           = 12;
        final int iREVISION_CONTRATO        = 13;
        final int iINCUMPLIM_CONTRATO       = 14;
        final int iREVISION_SALARIO         = 15;
        final int iREPARTO_UTILIDADES       = 16;
        final int iAPOYO_OTRA_HUELGA        = 17;
        final int iDESEQUILIBRIO_FAC_PROD   = 18;
        final int iOTRO_MOTIVO              = 19;
        final int iESPECIFIQUE_MOTIVO       = 20;

        final int iINCOMPETENCIA            = 21;
        final int iTIPO_INCOMPETENCIA       = 22;
        final int iESPECIFIQUE_INCOMP       = 23;
        final int iFECHA_PRESENTA_PETIC     = 24;

        final int iEMPLAZAMIENTO_HUELGA     = 27;
        final int iFECHA_EMPLAZAMIENTO      = 28;
        final int iPREHUELGA                = 29;
        final int iAUDIENCIA_CONCILIACION   = 30;
        final int iFECHA_AUDIENCIA          = 31;

        final int iESTALLAMIENTO_HUELGA     = 32;
        final int iDECLARA_LICITUD_HUELGA   = 33;
        final int iDECLARA_EXISTEN_HUELGA   = 34;

        final int iESTATUS_EXPEDIENTE       = 35;
        final int iFECHA_ACTO_PROCESAL      = 36;
        final int iFASE_SOLI_EXPEDIENTE     = 37;

        final int iFORMA_SOLUCION_EMPLAZ    = 38;
        final int iESPECIFI_FORMA_EMPLAZ    = 39;
        final int iFECHA_RESOLU_EMPLAZ      = 40;

        final int iFORMA_SOLUCION_HUELGA    = 43;
        final int iESPECIFI_FORMA_HUELGA    = 44;
        final int iFECHA_RESOLU_HUELGA      = 45;
        final int iTIPO_SENTENCIA           = 46;

        final int iFECHA_ESTALLAM_HUELGA    = 47;
        final int iFECHA_LEVANT_HUELGA      = 48;

        // ===== Defaults principales =====
        setIfNull(newRow, iTIPO_ASUNTO, 9);
        setIfNull(newRow, iRAMA_INDUS_INVOLUCRAD, "No Identificado");
        setIfNull(newRow, iSECTOR_RAMA, 99);
        setIfNull(newRow, iSUBSECTOR_RAMA, 99);

        setIfNull(newRow, iFIRMA_CONTRATO, 9);
        setIfNull(newRow, iREVISION_CONTRATO, 9);
        setIfNull(newRow, iINCUMPLIM_CONTRATO, 9);
        setIfNull(newRow, iREVISION_SALARIO, 9);
        setIfNull(newRow, iREPARTO_UTILIDADES, 9);
        setIfNull(newRow, iAPOYO_OTRA_HUELGA, 9);
        setIfNull(newRow, iDESEQUILIBRIO_FAC_PROD, 9);
        setIfNull(newRow, iOTRO_MOTIVO, 9);

        // Otro motivo => especifica
        Integer otroMotivo = asInt(newRow[iOTRO_MOTIVO]);
        if (otroMotivo != null && otroMotivo == 1) {
            setIfNull(newRow, iESPECIFIQUE_MOTIVO, "No Especifico");
        }

        // Incompetencia default
        setIfNull(newRow, iINCOMPETENCIA, 9);

        Integer incompetencia = asInt(newRow[iINCOMPETENCIA]);
        Integer tipoIncomp = asInt(newRow[iTIPO_INCOMPETENCIA]);

        // ===== Incompetencia = 1 (Sí) =====
        if (incompetencia != null && incompetencia == 1) {
            setIfNull(newRow, iTIPO_INCOMPETENCIA, 9);

            // Si tipo_incompetencia = 4 => especifique_incomp
            tipoIncomp = asInt(newRow[iTIPO_INCOMPETENCIA]);
            if (tipoIncomp != null && tipoIncomp == 4) {
                setIfNull(newRow, iESPECIFIQUE_INCOMP, "No identificado");
            }
        }

        // ===== Incompetencia = 2 (No) =====
        if (incompetencia != null && incompetencia == 2) {

            setIfNull(newRow, iFECHA_PRESENTA_PETIC, D_1899);

            // Emplazamiento
            setIfNull(newRow, iEMPLAZAMIENTO_HUELGA, 9);
            Integer emplaz = asInt(newRow[iEMPLAZAMIENTO_HUELGA]);
            if (emplaz != null && emplaz == 1) {
                setIfNull(newRow, iFECHA_EMPLAZAMIENTO, D_1899);
            }

            // Prehuelga + audiencia
            setIfNull(newRow, iPREHUELGA, 9);
            Integer prehuelga = asInt(newRow[iPREHUELGA]);

            if (prehuelga != null && prehuelga == 1) {
                setIfNull(newRow, iAUDIENCIA_CONCILIACION, 9);
            }

            Integer audienciaConc = asInt(newRow[iAUDIENCIA_CONCILIACION]);
            if (audienciaConc != null && audienciaConc == 1) {
                setIfNull(newRow, iFECHA_AUDIENCIA, D_1899);
            }

            // Estallamiento
            setIfNull(newRow, iESTALLAMIENTO_HUELGA, 9);
            Integer estall = asInt(newRow[iESTALLAMIENTO_HUELGA]);
            if (estall != null && estall == 1) {
                setIfNull(newRow, iDECLARA_LICITUD_HUELGA, 9);
                setIfNull(newRow, iDECLARA_EXISTEN_HUELGA, 9);
            }

            // Estatus expediente
            setIfNull(newRow, iESTATUS_EXPEDIENTE, 9);

            Integer estatusExp = asInt(newRow[iESTATUS_EXPEDIENTE]);

            // En proceso de resolución
            if (estatusExp != null && estatusExp == 2) {
                setIfNull(newRow, iFECHA_ACTO_PROCESAL, D_1899);
            }

            // Solucionado
            if (estatusExp != null && estatusExp == 1) {
                setIfNull(newRow, iFASE_SOLI_EXPEDIENTE, 99);

                Integer fase = asInt(newRow[iFASE_SOLI_EXPEDIENTE]);

                // Solución: emplazamiento (fase 5)
                if (fase != null && fase == 5) {
                    setIfNull(newRow, iFORMA_SOLUCION_EMPLAZ, 9);
                    setIfNull(newRow, iFECHA_RESOLU_EMPLAZ, D_1899);
                }

                // Solución: prehuelga (fase 6)
                if (fase != null && fase == 6) {
                    setIfNull(newRow, iFORMA_SOLUCION_EMPLAZ, 9);
                    setIfNull(newRow, iFECHA_RESOLU_EMPLAZ, D_1899);

                    Integer formaEmplaz = asInt(newRow[iFORMA_SOLUCION_EMPLAZ]);
                    if (formaEmplaz != null && formaEmplaz == 7) {
                        setIfNull(newRow, iESPECIFI_FORMA_EMPLAZ, "No Especifico");
                    }
                }

                // Solución: huelga (fase 7)
                if (fase != null && fase == 7) {
                    setIfNull(newRow, iFORMA_SOLUCION_HUELGA, 9);
                    setIfNull(newRow, iFECHA_RESOLU_HUELGA, D_1899);

                    Integer formaHuelga = asInt(newRow[iFORMA_SOLUCION_HUELGA]);

                    // Si hubo estallamiento = 1, fecha estallamiento
                    if (estall != null && estall == 1) {
                        setIfNull(newRow, iFECHA_ESTALLAM_HUELGA, D_1899);
                    }

                    setIfNull(newRow, iFECHA_LEVANT_HUELGA, D_1899);

                    if (formaHuelga != null && formaHuelga == 3) {
                        setIfNull(newRow, iTIPO_SENTENCIA, 9);
                    }

                    if (formaHuelga != null && formaHuelga == 5) {
                        setIfNull(newRow, iESPECIFI_FORMA_HUELGA, "No identificado");
                    }
                }
            }
        }

        // ===== Normalización 1999-09-09 -> 1899-09-09 =====
        replace1999With1899(newRow, iFECHA_APERTURA_EXPEDIENTE);
        replace1999With1899(newRow, iFECHA_PRESENTA_PETIC);
        replace1999With1899(newRow, iFECHA_EMPLAZAMIENTO);
        replace1999With1899(newRow, iFECHA_AUDIENCIA);
        replace1999With1899(newRow, iFECHA_ACTO_PROCESAL);
        replace1999With1899(newRow, iFECHA_RESOLU_EMPLAZ);
        replace1999With1899(newRow, iFECHA_RESOLU_HUELGA);
        replace1999With1899(newRow, iFECHA_ESTALLAM_HUELGA);
        replace1999With1899(newRow, iFECHA_LEVANT_HUELGA);
    }

    @Override public void close() { }
    @Override public void remove() { }
}
