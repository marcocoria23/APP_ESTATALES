/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Bean_Procedures;

import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/**
 *
 * @author ANTONIO.CORIA
 */
public class Ordinario extends Bean implements SQLData {

    private String SQLTypeName;
    private String NOMBRE_ORGANO_JURIS;
private String CLAVE_ORGANO;
private String EXPEDIENTE_CLAVE;
private String FECHA_APERTURA_EXPEDIENTE;
private String TIPO_ASUNTO;
private String NAT_CONFLICTO;
private String CONTRATO_ESCRITO;
private String TIPO_CONTRATO;
private String RAMA_INDUS_INVOLUCRADA;
private String SECTOR_RAMA;
private String SUBSECTOR_RAMA;
private String ENTIDAD_CLAVE;
private String ENTIDAD_NOMBRE;
private String MUNICIPIO_CLAVE;
private String MUNICIPIO_NOMBRE;
private String SUBCONTRATACION;
private String DESPIDO;
private String RESCISION_RL;
private String TERMINACION_RESCISION_RL;
private String VIOLACION_CONTRATO;
private String RIESGO_TRABAJO;
private String REVISION_CONTRATO;
private String PART_UTILIDADES;
private String OTRO_MOTIV_CONFLICTO;
private String OTRO_ESP_CONFLICTO;
private String CIRCUNS_MOTIVO_CONFL;
private String DETERM_EMPLEO_EMBARAZO;
private String DETERM_EMPLEO_EDAD;
private String DETERM_EMPLEO_GENERO;
private String DETERM_EMPLEO_ORIEN_SEX;
private String DETERM_EMPLEO_DISCAPACIDAD;
private String DETERM_EMPLEO_SOCIAL;
private String DETERM_EMPLEO_ORIGEN;
private String DETERM_EMPLEO_RELIGION;
private String DETERM_EMPLEO_MIGRA;
private String OTRO_DISCRIMINACION;
private String OTRO_ESP_DISCRIMI;
private String TRATA_LABORAL;
private String TRABAJO_FORZOSO;
private String TRABAJO_INFANTIL;
private String HOSTIGAMIENTO;
private String ACOSO_SEXUAL;
private String PAGO_PRESTACIONES;
private String INDEMNIZACION;
private String REINSTALACION;
private String SALARIO_RETENIDO;
private String AUMENTO_SALARIO;
private String DERECHO_ASCENSO;
private String DERECHO_PREFERENCIA;
private String DERECHO_ANTIGUEDAD;
private String OTRO_CONCEPTO;
private String OTRO_ESP_RECLAMADO;
private String AGUINALDO;
private String VACACIONES;
private String PRIMA_VACACIONAL;
private String PRIMA_ANTIGUEDAD;
private String OTRO_TIPO_PREST;
private String OTRO_ESP_PRESTAC;
private String MOTIVO_CONFLICTO_COLECT;
private String INCOMPETENCIA;
private String TIPO_INCOMPETENCIA;
private String OTRO_ESP_INCOMP;
private String FECHA_PRES_DEMANDA;
private String CONSTANCIA_CONS_EXPEDIDA;
private String CONSTANCIA_CLAVE;
private String ASUN_EXCEP_CONCILIACION;
private String PREVE_DEMANDA;
private String DESAHOGO_PREV_DEMANDA;
private String ESTATUS_DEMANDA;
private String CAU_IMP_ADM_DEMANDA;
private String FECHA_ADMI_DEMANDA;
private String CANTIDAD_ACTORES;
private String CANTIDAD_DEMANDADOS;
private String AUDIENCIA_PRELIM;
private String FECHA_AUDIENCIA_PRELIM;
private String AUDIENCIA_JUICIO;
private String FECHA_AUDIENCIA_JUICIO;
private String ESTATUS_EXPEDIENTE;
private String FECHA_ACTO_PROCESAL;
private String FASE_SOLI_EXPEDIENTE;
private String FORMA_SOLUCIONFE;
private String OTRO_ESP_SOLUCIONFE;
private String FECHA_DICTO_RESOLUCIONFE;
private String MONTO_SOLUCION_FE;
private String FORMA_SOLUCIONAP;
private String OTRO_ESP_SOLUCIONAP;
private String FECHA_DICTO_RESOLUCIONAP;
private String MONTO_SOLUCION_AP;
private String FORMA_SOLUCIONAJ;
private String OTRO_ESP_SOLUCIONAJ;
private String FECHA_RESOLUCIONAJ;
private String TIPO_SENTENCIAAJ;
private String MONTO_SOLUCIONAJ;
private String COMENTARIOS;
private String PERIODO;

    
  
    
    public Ordinario(){   
    }
    
    public String GetNOMBRE_ORGANO_JURIS() {
        return NOMBRE_ORGANO_JURIS;
    }

    public void SetNOMBRE_ORGANO_JURIS(String NOMBRE_ORGANO_JURIS) {
        this.NOMBRE_ORGANO_JURIS = NOMBRE_ORGANO_JURIS;
    }

    public String GetCLAVE_ORGANO() {
        return CLAVE_ORGANO;
    }

    public void SetCLAVE_ORGANO(String CLAVE_ORGANO) {
        this.CLAVE_ORGANO = CLAVE_ORGANO;
    }

    public String GetEXPEDIENTE_CLAVE() {
        return EXPEDIENTE_CLAVE;
    }

    public void SetEXPEDIENTE_CLAVE(String EXPEDIENTE_CLAVE) {
        this.EXPEDIENTE_CLAVE = EXPEDIENTE_CLAVE;
    }

    public String GetFECHA_APERTURA_EXPEDIENTE() {
        return FECHA_APERTURA_EXPEDIENTE;
    }

    public void SetFECHA_APERTURA_EXPEDIENTE(String FECHA_APERTURA_EXPEDIENTE) {
        this.FECHA_APERTURA_EXPEDIENTE = FECHA_APERTURA_EXPEDIENTE;
    }

    public String GetTIPO_ASUNTO() {
        return TIPO_ASUNTO;
    }

    public void SetTIPO_ASUNTO(String TIPO_ASUNTO) {
        this.TIPO_ASUNTO = TIPO_ASUNTO;
    }

    public String GetNAT_CONFLICTO() {
        return NAT_CONFLICTO;
    }

    public void SetNAT_CONFLICTO(String NAT_CONFLICTO) {
        this.NAT_CONFLICTO = NAT_CONFLICTO;
    }

    public String GetCONTRATO_ESCRITO() {
        return CONTRATO_ESCRITO;
    }

    public void SetCONTRATO_ESCRITO(String CONTRATO_ESCRITO) {
        this.CONTRATO_ESCRITO = CONTRATO_ESCRITO;
    }

    public String GetTIPO_CONTRATO() {
        return TIPO_CONTRATO;
    }

    public void SetTIPO_CONTRATO(String TIPO_CONTRATO) {
        this.TIPO_CONTRATO = TIPO_CONTRATO;
    }

    public String GetRAMA_INDUS_INVOLUCRADA() {
        return RAMA_INDUS_INVOLUCRADA;
    }

    public void SetRAMA_INDUS_INVOLUCRADA(String RAMA_INDUS_INVOLUCRADA) {
        this.RAMA_INDUS_INVOLUCRADA = RAMA_INDUS_INVOLUCRADA;
    }

    public String GetSECTOR_RAMA() {
        return SECTOR_RAMA;
    }

    public void SetSECTOR_RAMA(String SECTOR_RAMA) {
        this.SECTOR_RAMA = SECTOR_RAMA;
    }

    public String GetSUBSECTOR_RAMA() {
        return SUBSECTOR_RAMA;
    }

    public void SetSUBSECTOR_RAMA(String SUBSECTOR_RAMA) {
        this.SUBSECTOR_RAMA = SUBSECTOR_RAMA;
    }

    public String GetENTIDAD_CLAVE() {
        return ENTIDAD_CLAVE;
    }

    public void SetENTIDAD_CLAVE(String ENTIDAD_CLAVE) {
        this.ENTIDAD_CLAVE = ENTIDAD_CLAVE;
    }

    public String GetENTIDAD_NOMBRE() {
        return ENTIDAD_NOMBRE;
    }

    public void SetENTIDAD_NOMBRE(String ENTIDAD_NOMBRE) {
        this.ENTIDAD_NOMBRE = ENTIDAD_NOMBRE;
    }

    public String GetMUNICIPIO_CLAVE() {
        return MUNICIPIO_CLAVE;
    }

    public void SetMUNICIPIO_CLAVE(String MUNICIPIO_CLAVE) {
        this.MUNICIPIO_CLAVE = MUNICIPIO_CLAVE;
    }

    public String GetMUNICIPIO_NOMBRE() {
        return MUNICIPIO_NOMBRE;
    }

    public void SetMUNICIPIO_NOMBRE(String MUNICIPIO_NOMBRE) {
        this.MUNICIPIO_NOMBRE = MUNICIPIO_NOMBRE;
    }

    public String GetSUBCONTRATACION() {
        return SUBCONTRATACION;
    }

    public void SetSUBCONTRATACION(String SUBCONTRATACION) {
        this.SUBCONTRATACION = SUBCONTRATACION;
    }

    public String GetDESPIDO() {
        return DESPIDO;
    }

    public void SetDESPIDO(String DESPIDO) {
        this.DESPIDO = DESPIDO;
    }

    public String GetRESCISION_RL() {
        return RESCISION_RL;
    }

    public void SetRESCISION_RL(String RESCISION_RL) {
        this.RESCISION_RL = RESCISION_RL;
    }

    public String GetTERMINACION_RESCISION_RL() {
        return TERMINACION_RESCISION_RL;
    }

    public void SetTERMINACION_RESCISION_RL(String TERMINACION_RESCISION_RL) {
        this.TERMINACION_RESCISION_RL = TERMINACION_RESCISION_RL;
    }

    public String GetVIOLACION_CONTRATO() {
        return VIOLACION_CONTRATO;
    }

    public void SetVIOLACION_CONTRATO(String VIOLACION_CONTRATO) {
        this.VIOLACION_CONTRATO = VIOLACION_CONTRATO;
    }

    public String GetRIESGO_TRABAJO() {
        return RIESGO_TRABAJO;
    }

    public void SetRIESGO_TRABAJO(String RIESGO_TRABAJO) {
        this.RIESGO_TRABAJO = RIESGO_TRABAJO;
    }

    public String GetREVISION_CONTRATO() {
        return REVISION_CONTRATO;
    }

    public void SetREVISION_CONTRATO(String REVISION_CONTRATO) {
        this.REVISION_CONTRATO = REVISION_CONTRATO;
    }

    public String GetPART_UTILIDADES() {
        return PART_UTILIDADES;
    }

    public void SetPART_UTILIDADES(String PART_UTILIDADES) {
        this.PART_UTILIDADES = PART_UTILIDADES;
    }

    public String GetOTRO_MOTIV_CONFLICTO() {
        return OTRO_MOTIV_CONFLICTO;
    }

    public void SetOTRO_MOTIV_CONFLICTO(String OTRO_MOTIV_CONFLICTO) {
        this.OTRO_MOTIV_CONFLICTO = OTRO_MOTIV_CONFLICTO;
    }

    public String GetOTRO_ESP_CONFLICTO() {
        return OTRO_ESP_CONFLICTO;
    }

    public void SetOTRO_ESP_CONFLICTO(String OTRO_ESP_CONFLICTO) {
        this.OTRO_ESP_CONFLICTO = OTRO_ESP_CONFLICTO;
    }

    public String GetCIRCUNS_MOTIVO_CONFL() {
        return CIRCUNS_MOTIVO_CONFL;
    }

    public void SetCIRCUNS_MOTIVO_CONFL(String CIRCUNS_MOTIVO_CONFL) {
        this.CIRCUNS_MOTIVO_CONFL = CIRCUNS_MOTIVO_CONFL;
    }

    public String GetDETERM_EMPLEO_EMBARAZO() {
        return DETERM_EMPLEO_EMBARAZO;
    }

    public void SetDETERM_EMPLEO_EMBARAZO(String DETERM_EMPLEO_EMBARAZO) {
        this.DETERM_EMPLEO_EMBARAZO = DETERM_EMPLEO_EMBARAZO;
    }

    public String GetDETERM_EMPLEO_EDAD() {
        return DETERM_EMPLEO_EDAD;
    }

    public void SetDETERM_EMPLEO_EDAD(String DETERM_EMPLEO_EDAD) {
        this.DETERM_EMPLEO_EDAD = DETERM_EMPLEO_EDAD;
    }

    public String GetDETERM_EMPLEO_GENERO() {
        return DETERM_EMPLEO_GENERO;
    }

    public void SetDETERM_EMPLEO_GENERO(String DETERM_EMPLEO_GENERO) {
        this.DETERM_EMPLEO_GENERO = DETERM_EMPLEO_GENERO;
    }

    public String GetDETERM_EMPLEO_ORIEN_SEX() {
        return DETERM_EMPLEO_ORIEN_SEX;
    }

    public void SetDETERM_EMPLEO_ORIEN_SEX(String DETERM_EMPLEO_ORIEN_SEX) {
        this.DETERM_EMPLEO_ORIEN_SEX = DETERM_EMPLEO_ORIEN_SEX;
    }

    public String GetDETERM_EMPLEO_DISCAPACIDAD() {
        return DETERM_EMPLEO_DISCAPACIDAD;
    }

    public void SetDETERM_EMPLEO_DISCAPACIDAD(String DETERM_EMPLEO_DISCAPACIDAD) {
        this.DETERM_EMPLEO_DISCAPACIDAD = DETERM_EMPLEO_DISCAPACIDAD;
    }

    public String GetDETERM_EMPLEO_SOCIAL() {
        return DETERM_EMPLEO_SOCIAL;
    }

    public void SetDETERM_EMPLEO_SOCIAL(String DETERM_EMPLEO_SOCIAL) {
        this.DETERM_EMPLEO_SOCIAL = DETERM_EMPLEO_SOCIAL;
    }

    public String GetDETERM_EMPLEO_ORIGEN() {
        return DETERM_EMPLEO_ORIGEN;
    }

    public void SetDETERM_EMPLEO_ORIGEN(String DETERM_EMPLEO_ORIGEN) {
        this.DETERM_EMPLEO_ORIGEN = DETERM_EMPLEO_ORIGEN;
    }

    public String GetDETERM_EMPLEO_RELIGION() {
        return DETERM_EMPLEO_RELIGION;
    }

    public void SetDETERM_EMPLEO_RELIGION(String DETERM_EMPLEO_RELIGION) {
        this.DETERM_EMPLEO_RELIGION = DETERM_EMPLEO_RELIGION;
    }

    public String GetDETERM_EMPLEO_MIGRA() {
        return DETERM_EMPLEO_MIGRA;
    }

    public void SetDETERM_EMPLEO_MIGRA(String DETERM_EMPLEO_MIGRA) {
        this.DETERM_EMPLEO_MIGRA = DETERM_EMPLEO_MIGRA;
    }

    public String GetOTRO_DISCRIMINACION() {
        return OTRO_DISCRIMINACION;
    }

    public void SetOTRO_DISCRIMINACION(String OTRO_DISCRIMINACION) {
        this.OTRO_DISCRIMINACION = OTRO_DISCRIMINACION;
    }

    public String GetOTRO_ESP_DISCRIMI() {
        return OTRO_ESP_DISCRIMI;
    }

    public void SetOTRO_ESP_DISCRIMI(String OTRO_ESP_DISCRIMI) {
        this.OTRO_ESP_DISCRIMI = OTRO_ESP_DISCRIMI;
    }

    public String GetTRATA_LABORAL() {
        return TRATA_LABORAL;
    }

    public void SetTRATA_LABORAL(String TRATA_LABORAL) {
        this.TRATA_LABORAL = TRATA_LABORAL;
    }

    public String GetTRABAJO_FORZOSO() {
        return TRABAJO_FORZOSO;
    }

    public void SetTRABAJO_FORZOSO(String TRABAJO_FORZOSO) {
        this.TRABAJO_FORZOSO = TRABAJO_FORZOSO;
    }

    public String GetTRABAJO_INFANTIL() {
        return TRABAJO_INFANTIL;
    }

    public void SetTRABAJO_INFANTIL(String TRABAJO_INFANTIL) {
        this.TRABAJO_INFANTIL = TRABAJO_INFANTIL;
    }

    public String GetHOSTIGAMIENTO() {
        return HOSTIGAMIENTO;
    }

    public void SetHOSTIGAMIENTO(String HOSTIGAMIENTO) {
        this.HOSTIGAMIENTO = HOSTIGAMIENTO;
    }

    public String GetACOSO_SEXUAL() {
        return ACOSO_SEXUAL;
    }

    public void SetACOSO_SEXUAL(String ACOSO_SEXUAL) {
        this.ACOSO_SEXUAL = ACOSO_SEXUAL;
    }

    public String GetPAGO_PRESTACIONES() {
        return PAGO_PRESTACIONES;
    }

    public void SetPAGO_PRESTACIONES(String PAGO_PRESTACIONES) {
        this.PAGO_PRESTACIONES = PAGO_PRESTACIONES;
    }

    public String GetINDEMNIZACION() {
        return INDEMNIZACION;
    }

    public void SetINDEMNIZACION(String INDEMNIZACION) {
        this.INDEMNIZACION = INDEMNIZACION;
    }

    public String GetREINSTALACION() {
        return REINSTALACION;
    }

    public void SetREINSTALACION(String REINSTALACION) {
        this.REINSTALACION = REINSTALACION;
    }

    public String GetSALARIO_RETENIDO() {
        return SALARIO_RETENIDO;
    }

    public void SetSALARIO_RETENIDO(String SALARIO_RETENIDO) {
        this.SALARIO_RETENIDO = SALARIO_RETENIDO;
    }

    public String GetAUMENTO_SALARIO() {
        return AUMENTO_SALARIO;
    }

    public void SetAUMENTO_SALARIO(String AUMENTO_SALARIO) {
        this.AUMENTO_SALARIO = AUMENTO_SALARIO;
    }

    public String GetDERECHO_ASCENSO() {
        return DERECHO_ASCENSO;
    }

    public void SetDERECHO_ASCENSO(String DERECHO_ASCENSO) {
        this.DERECHO_ASCENSO = DERECHO_ASCENSO;
    }

    public String GetDERECHO_PREFERENCIA() {
        return DERECHO_PREFERENCIA;
    }

    public void SetDERECHO_PREFERENCIA(String DERECHO_PREFERENCIA) {
        this.DERECHO_PREFERENCIA = DERECHO_PREFERENCIA;
    }

    public String GetDERECHO_ANTIGUEDAD() {
        return DERECHO_ANTIGUEDAD;
    }

    public void SetDERECHO_ANTIGUEDAD(String DERECHO_ANTIGUEDAD) {
        this.DERECHO_ANTIGUEDAD = DERECHO_ANTIGUEDAD;
    }

    public String GetOTRO_CONCEPTO() {
        return OTRO_CONCEPTO;
    }

    public void SetOTRO_CONCEPTO(String OTRO_CONCEPTO) {
        this.OTRO_CONCEPTO = OTRO_CONCEPTO;
    }

    public String GetOTRO_ESP_RECLAMADO() {
        return OTRO_ESP_RECLAMADO;
    }

    public void SetOTRO_ESP_RECLAMADO(String OTRO_ESP_RECLAMADO) {
        this.OTRO_ESP_RECLAMADO = OTRO_ESP_RECLAMADO;
    }

    public String GetAGUINALDO() {
        return AGUINALDO;
    }

    public void SetAGUINALDO(String AGUINALDO) {
        this.AGUINALDO = AGUINALDO;
    }

    public String GetVACACIONES() {
        return VACACIONES;
    }

    public void SetVACACIONES(String VACACIONES) {
        this.VACACIONES = VACACIONES;
    }

    public String GetPRIMA_VACACIONAL() {
        return PRIMA_VACACIONAL;
    }

    public void SetPRIMA_VACACIONAL(String PRIMA_VACACIONAL) {
        this.PRIMA_VACACIONAL = PRIMA_VACACIONAL;
    }

    public String GetPRIMA_ANTIGUEDAD() {
        return PRIMA_ANTIGUEDAD;
    }

    public void SetPRIMA_ANTIGUEDAD(String PRIMA_ANTIGUEDAD) {
        this.PRIMA_ANTIGUEDAD = PRIMA_ANTIGUEDAD;
    }

    public String GetOTRO_TIPO_PREST() {
        return OTRO_TIPO_PREST;
    }

    public void SetOTRO_TIPO_PREST(String OTRO_TIPO_PREST) {
        this.OTRO_TIPO_PREST = OTRO_TIPO_PREST;
    }

    public String GetOTRO_ESP_PRESTAC() {
        return OTRO_ESP_PRESTAC;
    }

    public void SetOTRO_ESP_PRESTAC(String OTRO_ESP_PRESTAC) {
        this.OTRO_ESP_PRESTAC = OTRO_ESP_PRESTAC;
    }

    public String GetMOTIVO_CONFLICTO_COLECT() {
        return MOTIVO_CONFLICTO_COLECT;
    }

    public void SetMOTIVO_CONFLICTO_COLECT(String MOTIVO_CONFLICTO_COLECT) {
        this.MOTIVO_CONFLICTO_COLECT = MOTIVO_CONFLICTO_COLECT;
    }

    public String GetINCOMPETENCIA() {
        return INCOMPETENCIA;
    }

    public void SetINCOMPETENCIA(String INCOMPETENCIA) {
        this.INCOMPETENCIA = INCOMPETENCIA;
    }

    public String GetTIPO_INCOMPETENCIA() {
        return TIPO_INCOMPETENCIA;
    }

    public void SetTIPO_INCOMPETENCIA(String TIPO_INCOMPETENCIA) {
        this.TIPO_INCOMPETENCIA = TIPO_INCOMPETENCIA;
    }

    public String GetOTRO_ESP_INCOMP() {
        return OTRO_ESP_INCOMP;
    }

    public void SetOTRO_ESP_INCOMP(String OTRO_ESP_INCOMP) {
        this.OTRO_ESP_INCOMP = OTRO_ESP_INCOMP;
    }

    public String GetFECHA_PRES_DEMANDA() {
        return FECHA_PRES_DEMANDA;
    }

    public void SetFECHA_PRES_DEMANDA(String FECHA_PRES_DEMANDA) {
        this.FECHA_PRES_DEMANDA = FECHA_PRES_DEMANDA;
    }

    public String GetCONSTANCIA_CONS_EXPEDIDA() {
        return CONSTANCIA_CONS_EXPEDIDA;
    }

    public void SetCONSTANCIA_CONS_EXPEDIDA(String CONSTANCIA_CONS_EXPEDIDA) {
        this.CONSTANCIA_CONS_EXPEDIDA = CONSTANCIA_CONS_EXPEDIDA;
    }

    public String GetCONSTANCIA_CLAVE() {
        return CONSTANCIA_CLAVE;
    }

    public void SetCONSTANCIA_CLAVE(String CONSTANCIA_CLAVE) {
        this.CONSTANCIA_CLAVE = CONSTANCIA_CLAVE;
    }

    public String GetASUN_EXCEP_CONCILIACION() {
        return ASUN_EXCEP_CONCILIACION;
    }

    public void SetASUN_EXCEP_CONCILIACION(String ASUN_EXCEP_CONCILIACION) {
        this.ASUN_EXCEP_CONCILIACION = ASUN_EXCEP_CONCILIACION;
    }

    public String GetPREVE_DEMANDA() {
        return PREVE_DEMANDA;
    }

    public void SetPREVE_DEMANDA(String PREVE_DEMANDA) {
        this.PREVE_DEMANDA = PREVE_DEMANDA;
    }

    public String GetDESAHOGO_PREV_DEMANDA() {
        return DESAHOGO_PREV_DEMANDA;
    }

    public void SetDESAHOGO_PREV_DEMANDA(String DESAHOGO_PREV_DEMANDA) {
        this.DESAHOGO_PREV_DEMANDA = DESAHOGO_PREV_DEMANDA;
    }

    public String GetESTATUS_DEMANDA() {
        return ESTATUS_DEMANDA;
    }

    public void SetESTATUS_DEMANDA(String ESTATUS_DEMANDA) {
        this.ESTATUS_DEMANDA = ESTATUS_DEMANDA;
    }

    public String GetCAU_IMP_ADM_DEMANDA() {
        return CAU_IMP_ADM_DEMANDA;
    }

    public void SetCAU_IMP_ADM_DEMANDA(String CAU_IMP_ADM_DEMANDA) {
        this.CAU_IMP_ADM_DEMANDA = CAU_IMP_ADM_DEMANDA;
    }

    public String GetFECHA_ADMI_DEMANDA() {
        return FECHA_ADMI_DEMANDA;
    }

    public void SetFECHA_ADMI_DEMANDA(String FECHA_ADMI_DEMANDA) {
        this.FECHA_ADMI_DEMANDA = FECHA_ADMI_DEMANDA;
    }

    public String GetCANTIDAD_ACTORES() {
        return CANTIDAD_ACTORES;
    }

    public void SetCANTIDAD_ACTORES(String CANTIDAD_ACTORES) {
        this.CANTIDAD_ACTORES = CANTIDAD_ACTORES;
    }

    public String GetCANTIDAD_DEMANDADOS() {
        return CANTIDAD_DEMANDADOS;
    }

    public void SetCANTIDAD_DEMANDADOS(String CANTIDAD_DEMANDADOS) {
        this.CANTIDAD_DEMANDADOS = CANTIDAD_DEMANDADOS;
    }

    public String GetAUDIENCIA_PRELIM() {
        return AUDIENCIA_PRELIM;
    }

    public void SetAUDIENCIA_PRELIM(String AUDIENCIA_PRELIM) {
        this.AUDIENCIA_PRELIM = AUDIENCIA_PRELIM;
    }

    public String GetFECHA_AUDIENCIA_PRELIM() {
        return FECHA_AUDIENCIA_PRELIM;
    }

    public void SetFECHA_AUDIENCIA_PRELIM(String FECHA_AUDIENCIA_PRELIM) {
        this.FECHA_AUDIENCIA_PRELIM = FECHA_AUDIENCIA_PRELIM;
    }

    public String GetAUDIENCIA_JUICIO() {
        return AUDIENCIA_JUICIO;
    }

    public void SetAUDIENCIA_JUICIO(String AUDIENCIA_JUICIO) {
        this.AUDIENCIA_JUICIO = AUDIENCIA_JUICIO;
    }

    public String GetFECHA_AUDIENCIA_JUICIO() {
        return FECHA_AUDIENCIA_JUICIO;
    }

    public void SetFECHA_AUDIENCIA_JUICIO(String FECHA_AUDIENCIA_JUICIO) {
        this.FECHA_AUDIENCIA_JUICIO = FECHA_AUDIENCIA_JUICIO;
    }

    public String GetESTATUS_EXPEDIENTE() {
        return ESTATUS_EXPEDIENTE;
    }

    public void SetESTATUS_EXPEDIENTE(String ESTATUS_EXPEDIENTE) {
        this.ESTATUS_EXPEDIENTE = ESTATUS_EXPEDIENTE;
    }

    public String GetFECHA_ACTO_PROCESAL() {
        return FECHA_ACTO_PROCESAL;
    }

    public void SetFECHA_ACTO_PROCESAL(String FECHA_ACTO_PROCESAL) {
        this.FECHA_ACTO_PROCESAL = FECHA_ACTO_PROCESAL;
    }

    public String GetFASE_SOLI_EXPEDIENTE() {
        return FASE_SOLI_EXPEDIENTE;
    }

    public void SetFASE_SOLI_EXPEDIENTE(String FASE_SOLI_EXPEDIENTE) {
        this.FASE_SOLI_EXPEDIENTE = FASE_SOLI_EXPEDIENTE;
    }

    public String GetFORMA_SOLUCIONFE() {
        return FORMA_SOLUCIONFE;
    }

    public void SetFORMA_SOLUCIONFE(String FORMA_SOLUCIONFE) {
        this.FORMA_SOLUCIONFE = FORMA_SOLUCIONFE;
    }

    public String GetOTRO_ESP_SOLUCIONFE() {
        return OTRO_ESP_SOLUCIONFE;
    }

    public void SetOTRO_ESP_SOLUCIONFE(String OTRO_ESP_SOLUCIONFE) {
        this.OTRO_ESP_SOLUCIONFE = OTRO_ESP_SOLUCIONFE;
    }

    public String GetFECHA_DICTO_RESOLUCIONFE() {
        return FECHA_DICTO_RESOLUCIONFE;
    }

    public void SetFECHA_DICTO_RESOLUCIONFE(String FECHA_DICTO_RESOLUCIONFE) {
        this.FECHA_DICTO_RESOLUCIONFE = FECHA_DICTO_RESOLUCIONFE;
    }

    public String GetMONTO_SOLUCION_FE() {
        return MONTO_SOLUCION_FE;
    }

    public void SetMONTO_SOLUCION_FE(String MONTO_SOLUCION_FE) {
        this.MONTO_SOLUCION_FE = MONTO_SOLUCION_FE;
    }

    public String GetFORMA_SOLUCIONAP() {
        return FORMA_SOLUCIONAP;
    }

    public void SetFORMA_SOLUCIONAP(String FORMA_SOLUCIONAP) {
        this.FORMA_SOLUCIONAP = FORMA_SOLUCIONAP;
    }

    public String GetOTRO_ESP_SOLUCIONAP() {
        return OTRO_ESP_SOLUCIONAP;
    }

    public void SetOTRO_ESP_SOLUCIONAP(String OTRO_ESP_SOLUCIONAP) {
        this.OTRO_ESP_SOLUCIONAP = OTRO_ESP_SOLUCIONAP;
    }

    public String GetFECHA_DICTO_RESOLUCIONAP() {
        return FECHA_DICTO_RESOLUCIONAP;
    }

    public void SetFECHA_DICTO_RESOLUCIONAP(String FECHA_DICTO_RESOLUCIONAP) {
        this.FECHA_DICTO_RESOLUCIONAP = FECHA_DICTO_RESOLUCIONAP;
    }

    public String GetMONTO_SOLUCION_AP() {
        return MONTO_SOLUCION_AP;
    }

    public void SetMONTO_SOLUCION_AP(String MONTO_SOLUCION_AP) {
        this.MONTO_SOLUCION_AP = MONTO_SOLUCION_AP;
    }

    public String GetFORMA_SOLUCIONAJ() {
        return FORMA_SOLUCIONAJ;
    }

    public void SetFORMA_SOLUCIONAJ(String FORMA_SOLUCIONAJ) {
        this.FORMA_SOLUCIONAJ = FORMA_SOLUCIONAJ;
    }

    public String GetOTRO_ESP_SOLUCIONAJ() {
        return OTRO_ESP_SOLUCIONAJ;
    }

    public void SetOTRO_ESP_SOLUCIONAJ(String OTRO_ESP_SOLUCIONAJ) {
        this.OTRO_ESP_SOLUCIONAJ = OTRO_ESP_SOLUCIONAJ;
    }

    public String GetFECHA_RESOLUCIONAJ() {
        return FECHA_RESOLUCIONAJ;
    }

    public void SetFECHA_RESOLUCIONAJ(String FECHA_RESOLUCIONAJ) {
        this.FECHA_RESOLUCIONAJ = FECHA_RESOLUCIONAJ;
    }

    public String GetTIPO_SENTENCIAAJ() {
        return TIPO_SENTENCIAAJ;
    }

    public void SetTIPO_SENTENCIAAJ(String TIPO_SENTENCIAAJ) {
        this.TIPO_SENTENCIAAJ = TIPO_SENTENCIAAJ;
    }

    public String GetMONTO_SOLUCIONAJ() {
        return MONTO_SOLUCIONAJ;
    }

    public void SetMONTO_SOLUCIONAJ(String MONTO_SOLUCIONAJ) {
        this.MONTO_SOLUCIONAJ = MONTO_SOLUCIONAJ;
    }

    public String GetCOMENTARIOS() {
        return COMENTARIOS;
    }

    public void SetCOMENTARIOS(String COMENTARIOS) {
        this.COMENTARIOS = COMENTARIOS;
    }

    public String GetPERIODO() {
        return PERIODO;
    }

    public void SetPERIODO(String PERIODO) {
        this.PERIODO = PERIODO;
    }
  
      
      
   
      
    @Override
    public String getSQLTypeName() throws SQLException {
       return SQLTypeName;
         //throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object[] getLlave() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object[] toArray() {
        Object[] array = {
            GetNOMBRE_ORGANO_JURIS(),
            GetCLAVE_ORGANO(),
            GetEXPEDIENTE_CLAVE(),
            GetFECHA_APERTURA_EXPEDIENTE(),
            GetTIPO_ASUNTO(),
            GetNAT_CONFLICTO(),
            GetCONTRATO_ESCRITO(),
            GetTIPO_CONTRATO(),
            GetRAMA_INDUS_INVOLUCRADA(),
            GetSECTOR_RAMA(),
            GetSUBSECTOR_RAMA(),
            GetENTIDAD_CLAVE(),
            GetENTIDAD_NOMBRE(),
            GetMUNICIPIO_CLAVE(),
            GetMUNICIPIO_NOMBRE(),
            GetSUBCONTRATACION(),
            GetDESPIDO(),
            GetRESCISION_RL(),
            GetTERMINACION_RESCISION_RL(),
            GetVIOLACION_CONTRATO(),
            GetRIESGO_TRABAJO(),
            GetREVISION_CONTRATO(),
            GetPART_UTILIDADES(),
            GetOTRO_MOTIV_CONFLICTO(),
            GetOTRO_ESP_CONFLICTO(),
            GetCIRCUNS_MOTIVO_CONFL(),
            GetDETERM_EMPLEO_EMBARAZO(),
            GetDETERM_EMPLEO_EDAD(),
            GetDETERM_EMPLEO_GENERO(),
            GetDETERM_EMPLEO_ORIEN_SEX(),
            GetDETERM_EMPLEO_DISCAPACIDAD(),
            GetDETERM_EMPLEO_SOCIAL(),
            GetDETERM_EMPLEO_ORIGEN(),
            GetDETERM_EMPLEO_RELIGION(),
            GetDETERM_EMPLEO_MIGRA(),
            GetOTRO_DISCRIMINACION(),
            GetOTRO_ESP_DISCRIMI(),
            GetTRATA_LABORAL(),
            GetTRABAJO_FORZOSO(),
            GetTRABAJO_INFANTIL(),
            GetHOSTIGAMIENTO(),
            GetACOSO_SEXUAL(),
            GetPAGO_PRESTACIONES(),
            GetINDEMNIZACION(),
            GetREINSTALACION(),
            GetSALARIO_RETENIDO(),
            GetAUMENTO_SALARIO(),
            GetDERECHO_ASCENSO(),
            GetDERECHO_PREFERENCIA(),
            GetDERECHO_ANTIGUEDAD(),
            GetOTRO_CONCEPTO(),
            GetOTRO_ESP_RECLAMADO(),
            GetAGUINALDO(),
            GetVACACIONES(),
            GetPRIMA_VACACIONAL(),
            GetPRIMA_ANTIGUEDAD(),
            GetOTRO_TIPO_PREST(),
            GetOTRO_ESP_PRESTAC(),
            GetMOTIVO_CONFLICTO_COLECT(),
            GetINCOMPETENCIA(),
            GetTIPO_INCOMPETENCIA(),
            GetOTRO_ESP_INCOMP(),
            GetFECHA_PRES_DEMANDA(),
            GetCONSTANCIA_CONS_EXPEDIDA(),
            GetCONSTANCIA_CLAVE(),
            GetASUN_EXCEP_CONCILIACION(),
            GetPREVE_DEMANDA(),
            GetDESAHOGO_PREV_DEMANDA(),
            GetESTATUS_DEMANDA(),
            GetCAU_IMP_ADM_DEMANDA(),
            GetFECHA_ADMI_DEMANDA(),
            GetCANTIDAD_ACTORES(),
            GetCANTIDAD_DEMANDADOS(),
            GetAUDIENCIA_PRELIM(),
            GetFECHA_AUDIENCIA_PRELIM(),
            GetAUDIENCIA_JUICIO(),
            GetFECHA_AUDIENCIA_JUICIO(),
            GetESTATUS_EXPEDIENTE(),
            GetFECHA_ACTO_PROCESAL(),
            GetFASE_SOLI_EXPEDIENTE(),
            GetFORMA_SOLUCIONFE(),
            GetOTRO_ESP_SOLUCIONFE(),
            GetFECHA_DICTO_RESOLUCIONFE(),
            GetMONTO_SOLUCION_FE(),
            GetFORMA_SOLUCIONAP(),
            GetOTRO_ESP_SOLUCIONAP(),
            GetFECHA_DICTO_RESOLUCIONAP(),
            GetMONTO_SOLUCION_AP(),
            GetFORMA_SOLUCIONAJ(),
            GetOTRO_ESP_SOLUCIONAJ(),
            GetFECHA_RESOLUCIONAJ(),
            GetTIPO_SENTENCIAAJ(),
            GetMONTO_SOLUCIONAJ(),
            GetCOMENTARIOS(),
            GetPERIODO()
     };
     return array;
    }

    @Override
    public void setAtributos(Object[] obj) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void actualizaDesc(Connection conn) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    
private String ots(Object obj) {
        String val = null;
        if (obj != null) {
            val = obj.toString();
        }
        return val;
    }

    private Integer oti(Object obj) {
        Integer num = null;
        if (obj != null) {
            try {
                num = (Integer) obj;
            } catch (Exception ex) {
                System.out.println("Error en casteo de numero [" + obj + "]");
            }
        }
        return num;    
    }
    
}