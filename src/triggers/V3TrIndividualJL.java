package triggers;

import org.h2.api.Trigger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;

public class V3TrIndividualJL implements Trigger {

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

        // Por si te llega dd/MM/yyyy desde CSV (opcional)
        if (s.equals("09/09/1999")) return D_1999;
        if (s.equals("09/09/1899")) return D_1899;

        // Esperado: yyyy-mm-dd
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
        final int iCLAVE_ORGANO                = 1;
        final int iEXPEDIENTE_CLAVE            = 2;

        final int iFECHA_APERTURA_EXPEDIENTE   = 3;
        final int iTIPO_ASUNTO                 = 4;
        final int iNAT_CONFLICTO               = 5;

        final int iCONTRATO_ESCRITO            = 6;
        final int iTIPO_CONTRATO               = 7;

        final int iRAMA_INDUS_INVOLUCRADA      = 8;
        final int iSECTOR_RAMA                 = 9;
        final int iSUBSECTOR_RAMA              = 10;

        final int iSUBCONTRATACION             = 15;
        final int iINDOLE_TRABAJO              = 16;
        final int iPRESTACION_FP               = 17;
        final int iARRENDAM_TRAB               = 18;
        final int iCAPACITACION                = 19;
        final int iANTIGUEDAD                  = 20;
        final int iPRIMA_ANTIGUEDAD            = 21;
        final int iCONVENIO_TRAB               = 22;
        final int iDESIGNACION_TRAB_FALLE      = 23;
        final int iDESIGNACION_TRAB_ACT_DELIC  = 24;
        final int iTERMINACION_LAB             = 25;
        final int iRECUPERACION_CARGA          = 26;
        final int iGASTOS_TRASLADOS            = 27;
        final int iINDEMNIZACION               = 28;
        final int iPAGO_INDEMNIZACION          = 29;
        final int iDESACUERDO_MEDICOS          = 30;
        final int iCOBRO_PRESTACIONES          = 31;
        final int iCONF_SEGURO_SOCIAL          = 32;
        final int iOTRO_CONF                   = 33;
        final int iOTRO_ESP_CONF               = 34;

        final int iINCOMPETENCIA               = 35;
        final int iTIPO_INCOMPETENCIA          = 36;
        final int iOTRO_ESP_INCOMP             = 37;

        final int iFECHA_PRES_DEMANDA          = 38;
        final int iCONSTANCIA_CONS_EXPEDIDA    = 39;
        final int iCONSTANCIA_CLAVE            = 40;
        final int iASUN_EXCEP_CONCILIACION     = 41;

        final int iPREVE_DEMANDA               = 42;
        final int iDESAHOGO_PREV_DEMANDA       = 43;

        final int iESTATUS_DEMANDA             = 44;
        final int iCAU_IMPI_ADMI_DEMANDA       = 45;
        final int iFECHA_ADMI_DEMANDA          = 46;

        final int iTRAMITACION_DEPURACION      = 49;
        final int iFECHA_DEPURACION            = 50;

        final int iAUDIENCIA_PRELIM            = 51;
        final int iFECHA_AUDIENCIA_PRELIM      = 52;

        final int iAUDIENCIA_JUICIO            = 53;
        final int iFECHA_AUDIENCIA_JUICIO      = 54;

        final int iESTATUS_EXPEDIENTE          = 55;
        final int iFECHA_ACTO_PROCESAL         = 56;

        final int iFASE_SOLI_EXPEDIENTE        = 57;

        final int iFORMA_SOLUCION_AD           = 58;
        final int iOTRO_ESP_SOLUCION_AD        = 59;
        final int iFECHA_DICTO_RESOLUCION_AD   = 60;
        final int iTIPO_SENTENCIA_AD           = 61;

        final int iFORMA_SOLUCION_TA           = 63;
        final int iOTRO_ESP_SOLUCION_TA        = 64;
        final int iFECHA_RESOLUCION_TA         = 65;
        final int iTIPO_SENTENCIA_TA           = 66;

        final int iFORMA_SOLUCION_AP           = 68;
        final int iOTRO_ESP_SOLUCION_AP        = 69;
        final int iFECHA_DICTO_RESOLUCION_AP   = 70;

        final int iFORMA_SOLUCION_AJ           = 72;
        final int iOTRO_ESP_SOLUCION_AJ        = 73;
        final int iFECHA_DICTO_RESOLUCION_AJ   = 74;
        final int iTIPO_SENTENCIA_AJ           = 75;

        // ===== Defaults base =====
        setIfNull(newRow, iTIPO_ASUNTO, 9);
        setIfNull(newRow, iNAT_CONFLICTO, 9);

        setIfNull(newRow, iCONTRATO_ESCRITO, 9);
        Integer contratoEscrito = asInt(newRow[iCONTRATO_ESCRITO]);
        if (contratoEscrito != null && contratoEscrito == 1) {
            setIfNull(newRow, iTIPO_CONTRATO, 9);
        }

        setIfNull(newRow, iRAMA_INDUS_INVOLUCRADA, "No Identificado");
        setIfNull(newRow, iSECTOR_RAMA, 99);
        setIfNull(newRow, iSUBSECTOR_RAMA, 99);

        setIfNull(newRow, iSUBCONTRATACION, 9);
        setIfNull(newRow, iINDOLE_TRABAJO, 9);
        setIfNull(newRow, iPRESTACION_FP, 9);
        setIfNull(newRow, iARRENDAM_TRAB, 9);
        setIfNull(newRow, iCAPACITACION, 9);
        setIfNull(newRow, iANTIGUEDAD, 9);
        setIfNull(newRow, iPRIMA_ANTIGUEDAD, 9);
        setIfNull(newRow, iCONVENIO_TRAB, 9);
        setIfNull(newRow, iDESIGNACION_TRAB_FALLE, 9);
        setIfNull(newRow, iDESIGNACION_TRAB_ACT_DELIC, 9);
        setIfNull(newRow, iTERMINACION_LAB, 9);
        setIfNull(newRow, iRECUPERACION_CARGA, 9);
        setIfNull(newRow, iGASTOS_TRASLADOS, 9);
        setIfNull(newRow, iINDEMNIZACION, 9);
        setIfNull(newRow, iPAGO_INDEMNIZACION, 9);
        setIfNull(newRow, iDESACUERDO_MEDICOS, 9);
        setIfNull(newRow, iCOBRO_PRESTACIONES, 9);
        setIfNull(newRow, iCONF_SEGURO_SOCIAL, 9);

        setIfNull(newRow, iOTRO_CONF, 9);
        Integer otroConf = asInt(newRow[iOTRO_CONF]);
        if (otroConf != null && otroConf == 1) {
            setIfNull(newRow, iOTRO_ESP_CONF, "No Especifico");
        }

        // ===== Incompetencia =====
        setIfNull(newRow, iINCOMPETENCIA, 9);
        Integer incompetencia = asInt(newRow[iINCOMPETENCIA]);

        // Incompetencia Sí
        if (incompetencia != null && incompetencia == 1) {
            setIfNull(newRow, iTIPO_INCOMPETENCIA, 9);

            Integer tipoIncomp = asInt(newRow[iTIPO_INCOMPETENCIA]);
            if (tipoIncomp != null && tipoIncomp == 4) {
                setIfNull(newRow, iOTRO_ESP_INCOMP, "No Especifico");
            }
        }

        // Incompetencia No
        if (incompetencia != null && incompetencia == 2) {

            setIfNull(newRow, iFECHA_PRES_DEMANDA, D_1899);

            setIfNull(newRow, iCONSTANCIA_CONS_EXPEDIDA, 9);
            Integer constExp = asInt(newRow[iCONSTANCIA_CONS_EXPEDIDA]);

            // Nota: en tu Oracle aquí hay un posible error (setea OTRO_ESP_INCOMP)
            // lo replico tal cual: si constancia_cons_expedida=1 y constancia_clave NULL => OTRO_ESP_INCOMP='No Especifico'
            if (constExp != null && constExp == 1 && newRow[iCONSTANCIA_CLAVE] == null) {
                setIfNull(newRow, iOTRO_ESP_INCOMP, "No Especifico");
            }

            if (constExp != null && constExp == 2) {
                setIfNull(newRow, iASUN_EXCEP_CONCILIACION, 9);
            }

            setIfNull(newRow, iPREVE_DEMANDA, 9);
            Integer preve = asInt(newRow[iPREVE_DEMANDA]);
            if (preve != null && preve == 1) {
                setIfNull(newRow, iDESAHOGO_PREV_DEMANDA, 9);
            }

            setIfNull(newRow, iESTATUS_DEMANDA, 9);

            // Caso especial CLAVE_ORGANO='120351' y EXPEDIENTE_CLAVE='06/2022' => ESTATUS_DEMANDA = NULL
            String claveOrgano = (newRow[iCLAVE_ORGANO] == null) ? null : newRow[iCLAVE_ORGANO].toString();
            String expediente  = (newRow[iEXPEDIENTE_CLAVE] == null) ? null : newRow[iEXPEDIENTE_CLAVE].toString();

            if ("120351".equals(claveOrgano) && "06/2022".equals(expediente)) {
                newRow[iESTATUS_DEMANDA] = null;
            }

            Integer estDem = asInt(newRow[iESTATUS_DEMANDA]);

            if (estDem != null && estDem == 4) {
                setIfNull(newRow, iCAU_IMPI_ADMI_DEMANDA, 9);
            }

            if (estDem != null && estDem == 1) {
                setIfNull(newRow, iFECHA_ADMI_DEMANDA, D_1899);

                setIfNull(newRow, iTRAMITACION_DEPURACION, 9);
                Integer trDep = asInt(newRow[iTRAMITACION_DEPURACION]);
                if (trDep != null && trDep == 1) {
                    setIfNull(newRow, iFECHA_DEPURACION, D_1899);
                }

                setIfNull(newRow, iAUDIENCIA_PRELIM, 9);
                Integer audPre = asInt(newRow[iAUDIENCIA_PRELIM]);
                if (audPre != null && audPre == 1) {
                    setIfNull(newRow, iFECHA_AUDIENCIA_PRELIM, D_1899);
                }

                setIfNull(newRow, iAUDIENCIA_JUICIO, 9);
                Integer audJ = asInt(newRow[iAUDIENCIA_JUICIO]);
                if (audJ != null && audJ == 1) {
                    setIfNull(newRow, iFECHA_AUDIENCIA_JUICIO, D_1899);
                }

                // Nota: en tu Oracle aquí hay un posible error:
                // if ... ESTATUS_EXPEDIENTE IS NULL THEN :NEW.AUDIENCIA_JUICIO:=9;
                // lo replico literal:
                if (newRow[iESTATUS_EXPEDIENTE] == null) {
                    newRow[iAUDIENCIA_JUICIO] = 9;
                }

                Integer estExp = asInt(newRow[iESTATUS_EXPEDIENTE]);

                // En proceso de resolución
                if (estExp != null && estExp == 2) {
                    setIfNull(newRow, iFECHA_ACTO_PROCESAL, D_1899);
                }

                // Solucionado
                if (estExp != null && estExp == 1) {
                    setIfNull(newRow, iFASE_SOLI_EXPEDIENTE, 99);
                }

                Integer fase = asInt(newRow[iFASE_SOLI_EXPEDIENTE]);

                // Fase 3: Auto de depuración
                if (fase != null && fase == 3) {
                    setIfNull(newRow, iFORMA_SOLUCION_AD, 9);
                    setIfNull(newRow, iFECHA_DICTO_RESOLUCION_AD, D_1899);

                    Integer formaAD = asInt(newRow[iFORMA_SOLUCION_AD]);
                    if (formaAD != null && formaAD == 1) {
                        setIfNull(newRow, iTIPO_SENTENCIA_AD, 9);
                    }
                    if (formaAD != null && formaAD == 5) {
                        setIfNull(newRow, iOTRO_ESP_SOLUCION_AD, "No Especifico");
                    }
                }

                // Fase 4: Sin audiencias
                if (fase != null && fase == 4) {
                    setIfNull(newRow, iFORMA_SOLUCION_TA, 9);
                    setIfNull(newRow, iFECHA_RESOLUCION_TA, D_1899);

                    Integer formaTA = asInt(newRow[iFORMA_SOLUCION_TA]);
                    if (formaTA != null && formaTA == 1) {
                        setIfNull(newRow, iTIPO_SENTENCIA_TA, 9);
                    }
                    if (formaTA != null && formaTA == 5) {
                        setIfNull(newRow, iOTRO_ESP_SOLUCION_TA, "No Especifico");
                    }
                }

                // Fase 1: Audiencia preliminar
                if (fase != null && fase == 1) {
                    setIfNull(newRow, iFORMA_SOLUCION_AP, 9);
                    setIfNull(newRow, iFECHA_DICTO_RESOLUCION_AP, D_1899);

                    Integer formaAP = asInt(newRow[iFORMA_SOLUCION_AP]);
                    if (formaAP != null && formaAP == 5) {
                        setIfNull(newRow, iOTRO_ESP_SOLUCION_AP, "No Especifico");
                    }
                }

                // Fase 2: Audiencia de juicio
                if (fase != null && fase == 2) {
                    setIfNull(newRow, iFORMA_SOLUCION_AJ, 9);
                    setIfNull(newRow, iFECHA_DICTO_RESOLUCION_AJ, D_1899);

                    Integer formaAJ = asInt(newRow[iFORMA_SOLUCION_AJ]);
                    if (formaAJ != null && formaAJ == 1) {
                        setIfNull(newRow, iTIPO_SENTENCIA_AJ, 9);
                    }
                    if (formaAJ != null && formaAJ == 5) {
                        setIfNull(newRow, iOTRO_ESP_SOLUCION_AJ, "No Especifico");
                    }
                }
            }
        }

        // Caso especial:
        // if (CLAVE_ORGANO='270041') AND (EXPEDIENTE_CLAVE in ('237/2021','238/2021'))
        // THEN DESIGNACION_TRAB_FALLE := 1;
        String claveOrgano2 = (newRow[iCLAVE_ORGANO] == null) ? null : newRow[iCLAVE_ORGANO].toString();
        String expediente2  = (newRow[iEXPEDIENTE_CLAVE] == null) ? null : newRow[iEXPEDIENTE_CLAVE].toString();
        if ("270041".equals(claveOrgano2) &&
                ("237/2021".equals(expediente2) || "238/2021".equals(expediente2))) {
            newRow[iDESIGNACION_TRAB_FALLE] = 1;
        }
   // ===== Normalización 1999-09-09 -> 1899-09-09 =====
        replace1999With1899(newRow, iFECHA_APERTURA_EXPEDIENTE);
        replace1999With1899(newRow, iFECHA_PRES_DEMANDA);
        replace1999With1899(newRow, iFECHA_ADMI_DEMANDA);
        replace1999With1899(newRow, iFECHA_DEPURACION);
        replace1999With1899(newRow, iFECHA_AUDIENCIA_PRELIM);
        replace1999With1899(newRow, iFECHA_AUDIENCIA_JUICIO);
        replace1999With1899(newRow, iFECHA_ACTO_PROCESAL);
        replace1999With1899(newRow, iFECHA_DICTO_RESOLUCION_AD);
        replace1999With1899(newRow, iFECHA_RESOLUCION_TA);
        replace1999With1899(newRow, iFECHA_DICTO_RESOLUCION_AP);
        replace1999With1899(newRow, iFECHA_DICTO_RESOLUCION_AJ);
    }

    @Override public void close() { }
    @Override public void remove() { }
}