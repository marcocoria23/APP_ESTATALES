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
public class Individual extends Bean implements SQLData {

    private String SQLTypeName;
   private String	NOMBRE_ORGANO_JURIS;
private String	CLAVE_ORGANO;
private String	EXPEDIENTE_CLAVE;
private String	FECHA_APERTURA_EXPEDIENTE;
private String	TIPO_ASUNTO;
private String	NAT_CONFLICTO;
private String	CONTRATO_ESCRITO;
private String	TIPO_CONTRATO;
private String	RAMA_INDUS_INVOLUCRADA;
private String	SECTOR_RAMA;
private String	SUBSECTOR_RAMA;
private String	ENTIDAD_CLAVE;
private String	ENTIDAD_NOMBRE;
private String	MUNICIPIO_CLAVE;
private String	MUNICIPIO_NOMBRE;
private String	SUBCONTRATACION;
private String	INDOLE_TRABAJO;
private String	PRESTACION_FP;
private String	ARRENDAM_TRAB;
private String	CAPACITACION;
private String	ANTIGUEDAD;
private String	PRIMA_ANTIGUEDAD;
private String	CONVENIO_TRAB;
private String	DESIGNACION_TRAB_FALLE;
private String	DESIGNACION_TRAB_ACT_DELIC;
private String	TERMINACION_LAB;
private String	RECUPERACION_CARGA;
private String	GASTOS_TRASLADOS;
private String	INDEMNIZACION;
private String	PAGO_INDEMNIZACION;
private String	DESACUERDO_MEDICOS;
private String	COBRO_PRESTACIONES;
private String	CONF_SEGURO_SOCIAL;
private String	OTRO_CONF;
private String	OTRO_ESP_CONF;
private String	INCOMPETENCIA;
private String	TIPO_INCOMPETENCIA;
private String	OTRO_ESP_INCOMP;
private String	FECHA_PRES_DEMANDA;
private String	CONSTANCIA_CONS_EXPEDIDA;
private String	CONSTANCIA_CLAVE;
private String	ASUN_EXCEP_CONCILIACION;
private String	PREVE_DEMANDA;
private String	DESAHOGO_PREV_DEMANDA;
private String	ESTATUS_DEMANDA;
private String	CAU_IMPI_ADMI_DEMANDA;
private String	FECHA_ADMI_DEMANDA;
private String	CANTIDAD_ACTORES;
private String	CANTIDAD_DEMANDADOS;
private String	TRAMITACION_DEPURACION;
private String	FECHA_DEPURACION;
private String	AUDIENCIA_PRELIM;
private String	FECHA_AUDIENCIA_PRELIM;
private String	AUDIENCIA_JUICIO;
private String	FECHA_AUDIENCIA_JUICIO;
private String	ESTATUS_EXPEDIENTE;
private String	FECHA_ACTO_PROCESAL;
private String	FASE_SOLI_EXPEDIENTE;
private String	FORMA_SOLUCION_AD;
private String	OTRO_ESP_SOLUCION_AD;
private String	FECHA_DICTO_RESOLUCION_AD;
private String	TIPO_SENTENCIA_AD;
private String	MONTO_SOLUCION_AD;
private String	FORMA_SOLUCION_TA;
private String	OTRO_ESP_SOLUCION_TA;
private String	FECHA_RESOLUCION_TA;
private String	TIPO_SENTENCIA_TA;
private String	MONTO_SOLUCIÓN_TA;
private String	FORMA_SOLUCION_AP;
private String	OTRO_ESP_SOLUCION_AP;
private String	FECHA_DICTO_RESOLUCION_AP;
private String	MONTO_SOLUCION_AP;
private String	FORMA_SOLUCION_AJ;
private String	OTRO_ESP_SOLUCION_AJ;
private String	FECHA_DICTO_RESOLUCION_AJ;
private String	TIPO_SENTENCIA_AJ;
private String	MONTO_SOLUCIÓN_AJ;
private String	COMENTARIOS;

    
  
    
    public Individual(){   
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

    public String GetINDOLE_TRABAJO() {
        return INDOLE_TRABAJO;
    }

    public void SetINDOLE_TRABAJO(String INDOLE_TRABAJO) {
        this.INDOLE_TRABAJO = INDOLE_TRABAJO;
    }

    public String GetPRESTACION_FP() {
        return PRESTACION_FP;
    }

    public void SetPRESTACION_FP(String PRESTACION_FP) {
        this.PRESTACION_FP = PRESTACION_FP;
    }

    public String GetARRENDAM_TRAB() {
        return ARRENDAM_TRAB;
    }

    public void SetARRENDAM_TRAB(String ARRENDAM_TRAB) {
        this.ARRENDAM_TRAB = ARRENDAM_TRAB;
    }

    public String GetCAPACITACION() {
        return CAPACITACION;
    }

    public void SetCAPACITACION(String CAPACITACION) {
        this.CAPACITACION = CAPACITACION;
    }

    public String GetANTIGUEDAD() {
        return ANTIGUEDAD;
    }

    public void SetANTIGUEDAD(String ANTIGUEDAD) {
        this.ANTIGUEDAD = ANTIGUEDAD;
    }

    public String GetPRIMA_ANTIGUEDAD() {
        return PRIMA_ANTIGUEDAD;
    }

    public void SetPRIMA_ANTIGUEDAD(String PRIMA_ANTIGUEDAD) {
        this.PRIMA_ANTIGUEDAD = PRIMA_ANTIGUEDAD;
    }

    public String GetCONVENIO_TRAB() {
        return CONVENIO_TRAB;
    }

    public void SetCONVENIO_TRAB(String CONVENIO_TRAB) {
        this.CONVENIO_TRAB = CONVENIO_TRAB;
    }

    public String GetDESIGNACION_TRAB_FALLE() {
        return DESIGNACION_TRAB_FALLE;
    }

    public void SetDESIGNACION_TRAB_FALLE(String DESIGNACION_TRAB_FALLE) {
        this.DESIGNACION_TRAB_FALLE = DESIGNACION_TRAB_FALLE;
    }

    public String GetDESIGNACION_TRAB_ACT_DELIC() {
        return DESIGNACION_TRAB_ACT_DELIC;
    }

    public void SetDESIGNACION_TRAB_ACT_DELIC(String DESIGNACION_TRAB_ACT_DELIC) {
        this.DESIGNACION_TRAB_ACT_DELIC = DESIGNACION_TRAB_ACT_DELIC;
    }

    public String GetTERMINACION_LAB() {
        return TERMINACION_LAB;
    }

    public void SetTERMINACION_LAB(String TERMINACION_LAB) {
        this.TERMINACION_LAB = TERMINACION_LAB;
    }

    public String GetRECUPERACION_CARGA() {
        return RECUPERACION_CARGA;
    }

    public void SetRECUPERACION_CARGA(String RECUPERACION_CARGA) {
        this.RECUPERACION_CARGA = RECUPERACION_CARGA;
    }

    public String GetGASTOS_TRASLADOS() {
        return GASTOS_TRASLADOS;
    }

    public void SetGASTOS_TRASLADOS(String GASTOS_TRASLADOS) {
        this.GASTOS_TRASLADOS = GASTOS_TRASLADOS;
    }

    public String GetINDEMNIZACION() {
        return INDEMNIZACION;
    }

    public void SetINDEMNIZACION(String INDEMNIZACION) {
        this.INDEMNIZACION = INDEMNIZACION;
    }

    public String GetPAGO_INDEMNIZACION() {
        return PAGO_INDEMNIZACION;
    }

    public void SetPAGO_INDEMNIZACION(String PAGO_INDEMNIZACION) {
        this.PAGO_INDEMNIZACION = PAGO_INDEMNIZACION;
    }

    public String GetDESACUERDO_MEDICOS() {
        return DESACUERDO_MEDICOS;
    }

    public void SetDESACUERDO_MEDICOS(String DESACUERDO_MEDICOS) {
        this.DESACUERDO_MEDICOS = DESACUERDO_MEDICOS;
    }

    public String GetCOBRO_PRESTACIONES() {
        return COBRO_PRESTACIONES;
    }

    public void SetCOBRO_PRESTACIONES(String COBRO_PRESTACIONES) {
        this.COBRO_PRESTACIONES = COBRO_PRESTACIONES;
    }

    public String GetCONF_SEGURO_SOCIAL() {
        return CONF_SEGURO_SOCIAL;
    }

    public void SetCONF_SEGURO_SOCIAL(String CONF_SEGURO_SOCIAL) {
        this.CONF_SEGURO_SOCIAL = CONF_SEGURO_SOCIAL;
    }

    public String GetOTRO_CONF() {
        return OTRO_CONF;
    }

    public void SetOTRO_CONF(String OTRO_CONF) {
        this.OTRO_CONF = OTRO_CONF;
    }

    public String GetOTRO_ESP_CONF() {
        return OTRO_ESP_CONF;
    }

    public void SetOTRO_ESP_CONF(String OTRO_ESP_CONF) {
        this.OTRO_ESP_CONF = OTRO_ESP_CONF;
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

    public String GetCAU_IMPI_ADMI_DEMANDA() {
        return CAU_IMPI_ADMI_DEMANDA;
    }

    public void SetCAU_IMPI_ADMI_DEMANDA(String CAU_IMPI_ADMI_DEMANDA) {
        this.CAU_IMPI_ADMI_DEMANDA = CAU_IMPI_ADMI_DEMANDA;
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

    public String GetTRAMITACION_DEPURACION() {
        return TRAMITACION_DEPURACION;
    }

    public void SetTRAMITACION_DEPURACION(String TRAMITACION_DEPURACION) {
        this.TRAMITACION_DEPURACION = TRAMITACION_DEPURACION;
    }

    public String GetFECHA_DEPURACION() {
        return FECHA_DEPURACION;
    }

    public void SetFECHA_DEPURACION(String FECHA_DEPURACION) {
        this.FECHA_DEPURACION = FECHA_DEPURACION;
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

    public String GetFORMA_SOLUCION_AD() {
        return FORMA_SOLUCION_AD;
    }

    public void SetFORMA_SOLUCION_AD(String FORMA_SOLUCION_AD) {
        this.FORMA_SOLUCION_AD = FORMA_SOLUCION_AD;
    }

    public String GetOTRO_ESP_SOLUCION_AD() {
        return OTRO_ESP_SOLUCION_AD;
    }

    public void SetOTRO_ESP_SOLUCION_AD(String OTRO_ESP_SOLUCION_AD) {
        this.OTRO_ESP_SOLUCION_AD = OTRO_ESP_SOLUCION_AD;
    }

    public String GetFECHA_DICTO_RESOLUCION_AD() {
        return FECHA_DICTO_RESOLUCION_AD;
    }

    public void SetFECHA_DICTO_RESOLUCION_AD(String FECHA_DICTO_RESOLUCION_AD) {
        this.FECHA_DICTO_RESOLUCION_AD = FECHA_DICTO_RESOLUCION_AD;
    }

    public String GetTIPO_SENTENCIA_AD() {
        return TIPO_SENTENCIA_AD;
    }

    public void SetTIPO_SENTENCIA_AD(String TIPO_SENTENCIA_AD) {
        this.TIPO_SENTENCIA_AD = TIPO_SENTENCIA_AD;
    }

    public String GetMONTO_SOLUCION_AD() {
        return MONTO_SOLUCION_AD;
    }

    public void SetMONTO_SOLUCION_AD(String MONTO_SOLUCION_AD) {
        this.MONTO_SOLUCION_AD = MONTO_SOLUCION_AD;
    }

    public String GetFORMA_SOLUCION_TA() {
        return FORMA_SOLUCION_TA;
    }

    public void SetFORMA_SOLUCION_TA(String FORMA_SOLUCION_TA) {
        this.FORMA_SOLUCION_TA = FORMA_SOLUCION_TA;
    }

    public String GetOTRO_ESP_SOLUCION_TA() {
        return OTRO_ESP_SOLUCION_TA;
    }

    public void SetOTRO_ESP_SOLUCION_TA(String OTRO_ESP_SOLUCION_TA) {
        this.OTRO_ESP_SOLUCION_TA = OTRO_ESP_SOLUCION_TA;
    }

    public String GetFECHA_RESOLUCION_TA() {
        return FECHA_RESOLUCION_TA;
    }

    public void SetFECHA_RESOLUCION_TA(String FECHA_RESOLUCION_TA) {
        this.FECHA_RESOLUCION_TA = FECHA_RESOLUCION_TA;
    }

    public String GetTIPO_SENTENCIA_TA() {
        return TIPO_SENTENCIA_TA;
    }

    public void SetTIPO_SENTENCIA_TA(String TIPO_SENTENCIA_TA) {
        this.TIPO_SENTENCIA_TA = TIPO_SENTENCIA_TA;
    }

    public String GetMONTO_SOLUCIÓN_TA() {
        return MONTO_SOLUCIÓN_TA;
    }

    public void SetMONTO_SOLUCIÓN_TA(String MONTO_SOLUCIÓN_TA) {
        this.MONTO_SOLUCIÓN_TA = MONTO_SOLUCIÓN_TA;
    }

    public String GetFORMA_SOLUCION_AP() {
        return FORMA_SOLUCION_AP;
    }

    public void SetFORMA_SOLUCION_AP(String FORMA_SOLUCION_AP) {
        this.FORMA_SOLUCION_AP = FORMA_SOLUCION_AP;
    }

    public String GetOTRO_ESP_SOLUCION_AP() {
        return OTRO_ESP_SOLUCION_AP;
    }

    public void SetOTRO_ESP_SOLUCION_AP(String OTRO_ESP_SOLUCION_AP) {
        this.OTRO_ESP_SOLUCION_AP = OTRO_ESP_SOLUCION_AP;
    }

    public String GetFECHA_DICTO_RESOLUCION_AP() {
        return FECHA_DICTO_RESOLUCION_AP;
    }

    public void SetFECHA_DICTO_RESOLUCION_AP(String FECHA_DICTO_RESOLUCION_AP) {
        this.FECHA_DICTO_RESOLUCION_AP = FECHA_DICTO_RESOLUCION_AP;
    }

    public String GetMONTO_SOLUCION_AP() {
        return MONTO_SOLUCION_AP;
    }

    public void SetMONTO_SOLUCION_AP(String MONTO_SOLUCION_AP) {
        this.MONTO_SOLUCION_AP = MONTO_SOLUCION_AP;
    }

    public String GetFORMA_SOLUCION_AJ() {
        return FORMA_SOLUCION_AJ;
    }

    public void SetFORMA_SOLUCION_AJ(String FORMA_SOLUCION_AJ) {
        this.FORMA_SOLUCION_AJ = FORMA_SOLUCION_AJ;
    }

    public String GetOTRO_ESP_SOLUCION_AJ() {
        return OTRO_ESP_SOLUCION_AJ;
    }

    public void SetOTRO_ESP_SOLUCION_AJ(String OTRO_ESP_SOLUCION_AJ) {
        this.OTRO_ESP_SOLUCION_AJ = OTRO_ESP_SOLUCION_AJ;
    }

    public String GetFECHA_DICTO_RESOLUCION_AJ() {
        return FECHA_DICTO_RESOLUCION_AJ;
    }

    public void SetFECHA_DICTO_RESOLUCION_AJ(String FECHA_DICTO_RESOLUCION_AJ) {
        this.FECHA_DICTO_RESOLUCION_AJ = FECHA_DICTO_RESOLUCION_AJ;
    }

    public String GetTIPO_SENTENCIA_AJ() {
        return TIPO_SENTENCIA_AJ;
    }

    public void SetTIPO_SENTENCIA_AJ(String TIPO_SENTENCIA_AJ) {
        this.TIPO_SENTENCIA_AJ = TIPO_SENTENCIA_AJ;
    }

    public String GetMONTO_SOLUCIÓN_AJ() {
        return MONTO_SOLUCIÓN_AJ;
    }

    public void SetMONTO_SOLUCIÓN_AJ(String MONTO_SOLUCIÓN_AJ) {
        this.MONTO_SOLUCIÓN_AJ = MONTO_SOLUCIÓN_AJ;
    }

    public String GetCOMENTARIOS() {
        return COMENTARIOS;
    }

    public void SetCOMENTARIOS(String COMENTARIOS) {
        this.COMENTARIOS = COMENTARIOS;
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
            GetINDOLE_TRABAJO(),
            GetPRESTACION_FP(),
            GetARRENDAM_TRAB(),
            GetCAPACITACION(),
            GetANTIGUEDAD(),
            GetPRIMA_ANTIGUEDAD(),
            GetCONVENIO_TRAB(),
            GetDESIGNACION_TRAB_FALLE(),
            GetDESIGNACION_TRAB_ACT_DELIC(),
            GetTERMINACION_LAB(),
            GetRECUPERACION_CARGA(),
            GetGASTOS_TRASLADOS(),
            GetINDEMNIZACION(),
            GetPAGO_INDEMNIZACION(),
            GetDESACUERDO_MEDICOS(),
            GetCOBRO_PRESTACIONES(),
            GetCONF_SEGURO_SOCIAL(),
            GetOTRO_CONF(),
            GetOTRO_ESP_CONF(),
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
            GetCAU_IMPI_ADMI_DEMANDA(),
            GetFECHA_ADMI_DEMANDA(),
            GetCANTIDAD_ACTORES(),
            GetCANTIDAD_DEMANDADOS(),
            GetTRAMITACION_DEPURACION(),
            GetFECHA_DEPURACION(),
            GetAUDIENCIA_PRELIM(),
            GetFECHA_AUDIENCIA_PRELIM(),
            GetAUDIENCIA_JUICIO(),
            GetFECHA_AUDIENCIA_JUICIO(),
            GetESTATUS_EXPEDIENTE(),
            GetFECHA_ACTO_PROCESAL(),
            GetFASE_SOLI_EXPEDIENTE(),
            GetFORMA_SOLUCION_AD(),
            GetOTRO_ESP_SOLUCION_AD(),
            GetFECHA_DICTO_RESOLUCION_AD(),
            GetTIPO_SENTENCIA_AD(),
            GetMONTO_SOLUCION_AD(),
            GetFORMA_SOLUCION_TA(),
            GetOTRO_ESP_SOLUCION_TA(),
            GetFECHA_RESOLUCION_TA(),
            GetTIPO_SENTENCIA_TA(),
            GetMONTO_SOLUCIÓN_TA(),
            GetFORMA_SOLUCION_AP(),
            GetOTRO_ESP_SOLUCION_AP(),
            GetFECHA_DICTO_RESOLUCION_AP(),
            GetMONTO_SOLUCION_AP(),
            GetFORMA_SOLUCION_AJ(),
            GetOTRO_ESP_SOLUCION_AJ(),
            GetFECHA_DICTO_RESOLUCION_AJ(),
            GetTIPO_SENTENCIA_AJ(),
            GetMONTO_SOLUCIÓN_AJ(),
            GetCOMENTARIOS()
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
