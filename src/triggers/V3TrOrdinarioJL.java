package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;

/**
 * Trigger H2 equivalente a:
 * REL_2021.V3_TR_ORDINARIOJL  BEFORE INSERT FOR EACH ROW
 *
 * Incluye regla extra:
 *  - Si alguna fecha viene como 1999-09-09 => cambiar a 1899-09-09
 */
public class V3TrOrdinarioJL implements Trigger {

    private static final Date DATE_1899_09_09 = Date.valueOf("1899-09-09");
    private static final Date DATE_1999_09_09 = Date.valueOf("1999-09-09");

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
    if (newRow[idx] == null) {
        newRow[idx] = value;
        return;
    }

    String s = newRow[idx].toString().trim();
    if (s.isEmpty() || s.equalsIgnoreCase("Valor Cat No encontrado")) {
        newRow[idx] = value;
    }
}

    private void replace1999With1899(Object[] newRow, int idx) {
        Object v = newRow[idx];
        if (v == null) return;

        // Normalmente H2 usa java.sql.Date en columnas DATE
        if (v instanceof Date) {
            if (DATE_1999_09_09.equals(v)) newRow[idx] = DATE_1899_09_09;
            return;
        }

        // Si por alguna razón llega como String
        String s = v.toString().trim();
        if (s.equals("09/09/1999") || s.equals("1999-09-09")) {
            newRow[idx] = DATE_1899_09_09;
        }
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
try {
        // ===== Índices (0-based) según el CREATE TABLE que pegaste =====
        // 0 NOMBRE_ORGANO_JURIS
        // 1 CLAVE_ORGANO
        // 2 EXPEDIENTE_CLAVE
        final int iFECHA_APERTURA_EXPEDIENTE = 3;

        final int iTIPO_ASUNTO = 4;
        final int iNAT_CONFLICTO = 5;

        final int iCONTRATO_ESCRITO = 6;
        final int iTIPO_CONTRATO = 7;

        final int iRAMA_INDUS_INVOLUCRADA = 8;
        final int iSECTOR_RAMA = 9;
        final int iSUBSECTOR_RAMA = 10;

        final int iSUBCONTRATACION = 15;
        final int iDESPIDO = 16;
        final int iRESCISION_RL = 17;
        final int iTERMINACION_RESCISION_RL = 18;
        final int iVIOLACION_CONTRATO = 19;
        final int iRIESGO_TRABAJO = 20;
        final int iREVISION_CONTRATO = 21;
        final int iPART_UTILIDADES = 22;
        final int iOTRO_MOTIV_CONFLICTO = 23;
        final int iOTRO_ESP_CONFLICTO = 24;

        final int iCIRCUNS_MOTIVO_CONFL = 25;

        final int iDETERM_EMPLEO_EMBARAZO = 26;
        final int iDETERM_EMPLEO_EDAD = 27;
        final int iDETERM_EMPLEO_GENERO = 28;
        final int iDETERM_EMPLEO_ORIEN_SEX = 29;
        final int iDETERM_EMPLEO_DISCAPACIDAD = 30;
        final int iDETERM_EMPLEO_SOCIAL = 31;
        final int iDETERM_EMPLEO_ORIGEN = 32;
        final int iDETERM_EMPLEO_RELIGION = 33;
        final int iDETERM_EMPLEO_MIGRA = 34;

        final int iOTRO_DISCRIMINACION = 35;
        final int iOTRO_ESP_DISCRIMI = 36;

        final int iTRATA_LABORAL = 37;
        final int iTRABAJO_FORZOSO = 38;
        final int iTRABAJO_INFANTIL = 39;
        final int iHOSTIGAMIENTO = 40;
        final int iACOSO_SEXUAL = 41;

        final int iPAGO_PRESTACIONES = 42;
        final int iINDEMNIZACION = 43;
        final int iREINSTALACION = 44;
        final int iSALARIO_RETENIDO = 45;
        final int iAUMENTO_SALARIO = 46;
        final int iDERECHO_ASCENSO = 47;
        final int iDERECHO_PREFERENCIA = 48;
        final int iDERECHO_ANTIGUEDAD = 49;

        final int iOTRO_CONCEPTO = 50;
        final int iOTRO_ESP_RECLAMADO = 51;

        final int iAGUINALDO = 52;
        final int iVACACIONES = 53;
        final int iPRIMA_VACACIONAL = 54;
        final int iPRIMA_ANTIGUEDAD = 55;
        final int iOTRO_TIPO_PREST = 56;
        final int iOTRO_ESP_PRESTAC = 57;

        final int iMOTIVO_CONFLICTO_COLECT = 58;

        final int iINCOMPETENCIA = 59;
        final int iTIPO_INCOMPETENCIA = 60;
        final int iFECHA_PRES_DEMANDA = 62;
        final int iCONSTANCIA_CONS_EXPEDIDA = 63;
        final int iPREVE_DEMANDA = 66;
        final int iDESAHOGO_PREV_DEMANDA = 67;
        final int iESTATUS_DEMANDA = 68;
        final int iCAU_IMP_ADM_DEMANDA = 69;
        final int iFECHA_ADMI_DEMANDA = 70;

        final int iAUDIENCIA_PRELIM = 73;
        final int iFECHA_AUDIENCIA_PRELIM = 74;

        final int iAUDIENCIA_JUICIO = 75;
        final int iFECHA_AUDIENCIA_JUICIO = 76;

        final int iESTATUS_EXPEDIENTE = 77;
        final int iFECHA_ACTO_PROCESAL = 78;

        final int iFASE_SOLI_EXPEDIENTE = 79;

        final int iFORMA_SOLUCIONFE = 80;
        final int iOTRO_ESP_SOLUCIONFE = 81;
        final int iFECHA_DICTO_RESOLUCIONFE = 82;

        final int iFORMA_SOLUCIONAP = 84;
        final int iOTRO_ESP_SOLUCIONAP = 85;
        final int iFECHA_DICTO_RESOLUCIONAP = 86;

        final int iFORMA_SOLUCIONAJ = 88;
        final int iOTRO_ESP_SOLUCIONAJ = 89;
        final int iFECHA_RESOLUCIONAJ = 90;

        // ===== Lógica (misma que tu PL/SQL) =====

        Integer tipoAsunto = asInt(newRow[iTIPO_ASUNTO]);

        if (tipoAsunto != null && tipoAsunto == 1) {

            setIfNull(newRow, iCONTRATO_ESCRITO, 9);

            // si CONTRATO_ESCRITO=1 y TIPO_CONTRATO null => 9
            Integer contratoEscrito = asInt(newRow[iCONTRATO_ESCRITO]);
            if (contratoEscrito != null && contratoEscrito == 1) {
                setIfNull(newRow, iTIPO_CONTRATO, 9);
            }

            setIfNull(newRow, iSUBCONTRATACION, 9);
            setIfNull(newRow, iDESPIDO, 9);
            setIfNull(newRow, iRESCISION_RL, 9);
            setIfNull(newRow, iTERMINACION_RESCISION_RL, 9);
            setIfNull(newRow, iVIOLACION_CONTRATO, 9);
            setIfNull(newRow, iRIESGO_TRABAJO, 9);
            setIfNull(newRow, iREVISION_CONTRATO, 9);
            setIfNull(newRow, iPART_UTILIDADES, 9);
            setIfNull(newRow, iOTRO_MOTIV_CONFLICTO, 9);

            Integer otroMotiv = asInt(newRow[iOTRO_MOTIV_CONFLICTO]);
            if (otroMotiv != null && otroMotiv == 1) {
                setIfNull(newRow, iOTRO_ESP_CONFLICTO, "No Especifico");
            }

            setIfNull(newRow, iCIRCUNS_MOTIVO_CONFL, 9);

            Integer circuns = asInt(newRow[iCIRCUNS_MOTIVO_CONFL]);
            if (circuns != null && circuns == 1) {
                setIfNull(newRow, iDETERM_EMPLEO_EMBARAZO, 9);
                setIfNull(newRow, iDETERM_EMPLEO_EDAD, 9);
                setIfNull(newRow, iDETERM_EMPLEO_GENERO, 9);
                setIfNull(newRow, iDETERM_EMPLEO_ORIEN_SEX, 9);
                setIfNull(newRow, iDETERM_EMPLEO_DISCAPACIDAD, 9);
                setIfNull(newRow, iDETERM_EMPLEO_SOCIAL, 9);
                setIfNull(newRow, iDETERM_EMPLEO_ORIGEN, 9);
                setIfNull(newRow, iDETERM_EMPLEO_RELIGION, 9);
                setIfNull(newRow, iDETERM_EMPLEO_MIGRA, 9);

                setIfNull(newRow, iOTRO_DISCRIMINACION, 9);

                Integer otroDis = asInt(newRow[iOTRO_DISCRIMINACION]);
                if (otroDis != null && otroDis == 1) {
                    setIfNull(newRow, iOTRO_ESP_DISCRIMI, "No Especifico");
                }

                setIfNull(newRow, iTRATA_LABORAL, 9);
                setIfNull(newRow, iTRABAJO_FORZOSO, 9);
                setIfNull(newRow, iTRABAJO_INFANTIL, 9);
                setIfNull(newRow, iHOSTIGAMIENTO, 9);
                setIfNull(newRow, iACOSO_SEXUAL, 9);
            }

            setIfNull(newRow, iPAGO_PRESTACIONES, 9);
            setIfNull(newRow, iINDEMNIZACION, 9);
            setIfNull(newRow, iREINSTALACION, 9);
            setIfNull(newRow, iSALARIO_RETENIDO, 9);
            setIfNull(newRow, iAUMENTO_SALARIO, 9);
            setIfNull(newRow, iDERECHO_ASCENSO, 9);
            setIfNull(newRow, iDERECHO_PREFERENCIA, 9);
            setIfNull(newRow, iDERECHO_ANTIGUEDAD, 9);

            setIfNull(newRow, iOTRO_CONCEPTO, 9);

            Integer otroConcepto = asInt(newRow[iOTRO_CONCEPTO]);
            if (otroConcepto != null && otroConcepto == 1) {
                setIfNull(newRow, iOTRO_ESP_RECLAMADO, "No Especifico");
            }

            Integer pagoPrest = asInt(newRow[iPAGO_PRESTACIONES]);
            if (pagoPrest != null && pagoPrest == 1) {
                setIfNull(newRow, iAGUINALDO, 9);
                setIfNull(newRow, iVACACIONES, 9);
                setIfNull(newRow, iPRIMA_VACACIONAL, 9);
                setIfNull(newRow, iPRIMA_ANTIGUEDAD, 9);
                setIfNull(newRow, iOTRO_TIPO_PREST, 9);

                Integer otroTipoPrest = asInt(newRow[iOTRO_TIPO_PREST]);
                if (otroTipoPrest != null && otroTipoPrest == 1) {
                    setIfNull(newRow, iOTRO_ESP_PRESTAC, "No Especifico");
                }
            }
        }

        // NAT_CONFLICTO null => 9
        setIfNull(newRow, iNAT_CONFLICTO, 9);

        // RAMA_INDUS_INVOLUCRADA null => 'No Identificado' ; sector/subsector null => 99
        setIfNull(newRow, iRAMA_INDUS_INVOLUCRADA, "No Identificado");
        setIfNull(newRow, iSECTOR_RAMA, 99);
        setIfNull(newRow, iSUBSECTOR_RAMA, 99);

        // Si tipo_asunto = 2 y motivo_conflicto_colect null => 'No Identificado'
        if (tipoAsunto != null && tipoAsunto == 2) {
            setIfNull(newRow, iMOTIVO_CONFLICTO_COLECT, "No Identificado");
        }

        // if INCOMPETENCIA is null => 9
        setIfNull(newRow, iINCOMPETENCIA, 9);

        Integer inc = asInt(newRow[iINCOMPETENCIA]);

        if (inc != null && inc == 1) {
            setIfNull(newRow, iTIPO_INCOMPETENCIA, 9);
        }

        if (inc != null && inc == 2) {

            setIfNull(newRow, iFECHA_PRES_DEMANDA, DATE_1899_09_09);
            setIfNull(newRow, iCONSTANCIA_CONS_EXPEDIDA, 9);
            setIfNull(newRow, iPREVE_DEMANDA, 9);

            Integer preve = asInt(newRow[iPREVE_DEMANDA]);
            if (preve != null && preve == 1) {
                setIfNull(newRow, iDESAHOGO_PREV_DEMANDA, 9);
            }

            setIfNull(newRow, iESTATUS_DEMANDA, 9);

            Integer estDem = asInt(newRow[iESTATUS_DEMANDA]);

            // estatus_demanda in (2,3,4) y CAU_IMP... null => 9
            if (estDem != null && (estDem == 2 || estDem == 3 || estDem == 4)) {
                setIfNull(newRow, iCAU_IMP_ADM_DEMANDA, 9);
            }

            // estatus_demanda=1 => fecha_admi_demanda default + audiencia_prelim + fechas etc.
            if (estDem != null && estDem == 1) {

                setIfNull(newRow, iFECHA_ADMI_DEMANDA, DATE_1899_09_09);
                setIfNull(newRow, iAUDIENCIA_PRELIM, 9);

                Integer audPre = asInt(newRow[iAUDIENCIA_PRELIM]);
                if (audPre != null && audPre == 1) {
                    setIfNull(newRow, iFECHA_AUDIENCIA_PRELIM, DATE_1899_09_09);
                }

                Integer audJui = asInt(newRow[iAUDIENCIA_JUICIO]);
                if (audJui != null && audJui == 1) {
                    setIfNull(newRow, iFECHA_AUDIENCIA_JUICIO, DATE_1899_09_09);
                }

                setIfNull(newRow, iESTATUS_EXPEDIENTE, 9);

                Integer estExp = asInt(newRow[iESTATUS_EXPEDIENTE]);

                if (estExp != null && estExp == 2) {
                    setIfNull(newRow, iFECHA_ACTO_PROCESAL, DATE_1899_09_09);
                }

                if (estExp != null && estExp == 1) {
                    setIfNull(newRow, iFASE_SOLI_EXPEDIENTE, 99);

                    Integer fase = asInt(newRow[iFASE_SOLI_EXPEDIENTE]);

                    // OJO: tu script trae lógica para fase=9 (FORMA_SOLUCIONFE...) pero
                    // en tu CREATE TABLE sí existe FORMA_SOLUCIONFE/FECHA_DICTO_RESOLUCIONFE etc.
                    if (fase != null && fase == 9) {
                        setIfNull(newRow, iFORMA_SOLUCIONFE, 9);
                        Integer formaFE = asInt(newRow[iFORMA_SOLUCIONFE]);

                        if (formaFE != null && (formaFE == 2 || formaFE == 3 || formaFE == 4)) {
                            setIfNull(newRow, iFECHA_DICTO_RESOLUCIONFE, DATE_1899_09_09);
                        }
                        if (formaFE != null && formaFE == 5) {
                            setIfNull(newRow, iOTRO_ESP_SOLUCIONFE, "No Especifico");
                            setIfNull(newRow, iFECHA_DICTO_RESOLUCIONFE, DATE_1899_09_09);
                        }
                    }

                    // fase=1 => AP
                    if (fase != null && fase == 1) {
                        setIfNull(newRow, iFORMA_SOLUCIONAP, 9);
                        Integer formaAP = asInt(newRow[iFORMA_SOLUCIONAP]);

                        if (formaAP != null && (formaAP == 2 || formaAP == 3 || formaAP == 4)) {
                            setIfNull(newRow, iFECHA_DICTO_RESOLUCIONAP, DATE_1899_09_09);
                        }
                        if (formaAP != null && formaAP == 5) {
                            setIfNull(newRow, iOTRO_ESP_SOLUCIONAP, "No Especifico");
                            setIfNull(newRow, iFECHA_DICTO_RESOLUCIONAP, DATE_1899_09_09);
                        }
                    }

                    // fase=2 => AJ
                    if (fase != null && fase == 2) {
                        setIfNull(newRow, iFORMA_SOLUCIONAJ, 9);
                        Integer formaAJ = asInt(newRow[iFORMA_SOLUCIONAJ]);

                        if (formaAJ != null && (formaAJ == 1 || formaAJ == 2 || formaAJ == 3 || formaAJ == 4)) {
                            setIfNull(newRow, iFECHA_RESOLUCIONAJ, DATE_1899_09_09);
                        }
                        if (formaAJ != null && formaAJ == 5) {
                            setIfNull(newRow, iOTRO_ESP_SOLUCIONAJ, "No Especifico");
                        }
                    }
                }
            }
        }

        // ===== Regla extra: si llega 1999-09-09 => cambiar a 1899-09-09 =====
        replace1999With1899(newRow, iFECHA_APERTURA_EXPEDIENTE);
        replace1999With1899(newRow, iFECHA_PRES_DEMANDA);
        replace1999With1899(newRow, iFECHA_ADMI_DEMANDA);
        replace1999With1899(newRow, iFECHA_AUDIENCIA_PRELIM);
        replace1999With1899(newRow, iFECHA_AUDIENCIA_JUICIO);
        replace1999With1899(newRow, iFECHA_ACTO_PROCESAL);
        replace1999With1899(newRow, iFECHA_DICTO_RESOLUCIONFE);
        replace1999With1899(newRow, iFECHA_DICTO_RESOLUCIONAP);
        replace1999With1899(newRow, iFECHA_RESOLUCIONAJ);
        
     } catch (Exception e) {
            System.out.println("EXCEPCION en trigger: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace(System.out); // <-- AQUI veras la linea exacta
            throw e; // <-- importante: no te comas el error
        }

}

    @Override public void close() { }
    @Override public void remove() { }
}
