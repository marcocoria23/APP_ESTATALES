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
public class Colectivo extends Bean implements SQLData {

    private String SQLTypeName;
    private String NOMBRE_ORGANO_JURIS;
    private String CLAVE_ORGANO;
    private String EXPEDIENTE_CLAVE;
    private String FECHA_APERTURA_EXPEDIENTE;
    private String TIPO_ASUNTO;
    private String NAT_CONFLICTO;
    private String RAMA_INDUS_INVOLUCRAD;
    private String SECTOR_RAMA;
    private String SUBSECTOR_RAMA;
    private String ENTIDAD_CLAVE;
    private String ENTIDAD_NOMBRE;
    private String MUNICIPIO_CLAVE;
    private String MUNICIPIO_NOMBRE;
    private String DECLARACION_PERDIDA_MAY;
    private String SUSPENSION_TMP;
    private String TERMINACION_TRAB;
    private String CONTRATACION_COLECTIVA;
    private String OMISIONES_REGLAMENTO;
    private String REDUCCION_PERSONAL;
    private String VIOLA_DERECHOS;
    private String ELECCION_SINDICALES;
    private String SANCION_SINDICALES;
    private String OTRO_CONFLICTO;
    private String OTRO_ESP_CONFLICTO;
    private String NO_IMPUTABLE_ST;
    private String INCAPACIDAD_FISICA_ST;
    private String FALTA_MATERIA_PRIM_ST;
    private String FALTA_MINISTRACION_ST;
    private String FUERZA_MAYOR_TC;
    private String INCAPACIDAD_FISICA_TC;
    private String QUIEBRA_LEGAL_TC;
    private String AGOTAMIENTO_MATERIA_TC;
    private String LIBERTAD_ASOCIACION;
    private String LIBERTAD_SINDICAL;
    private String DERECHO_COLECTIVA;
    private String OTRO_COLECTIVA;
    private String OTRO_ESP_COLECTIVA;
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
    private String FECHA_ADMI_DEMANDA;
    private String CANTIDAD_ACTORES;
    private String CANTIDAD_DEMANDADOS;
    private String AUTO_DEPURACION;
    private String FECHA_DEPURACION;
    private String AUDIENCIA_JUICIO;
    private String FECHA_AUDIENCIA_JUICIO;
    private String ESTATUS_EXPEDIENTE;
    private String FECHA_ACTO_PROCESAL;
    private String FASE_SOLI_EXPEDIENTE;
    private String FORMA_SOLUCION_AD;
    private String OTRO_ESP_SOLUCION_AD;
    private String FECHA_DICTO_RESOLUCION_AD;
    private String TIPO_SENTENCIA_AD;
    private String MONTO_SOLUCION_AD;
    private String FORMA_SOLUCION_AJ;
    private String OTRO_ESP_SOLUCION_AJ;
    private String FECHA_RESOLUCION_AJ;
    private String TIPO_SENTENCIA_AJ;
    private String MONTO_SOLUCION_AJ;
    private String COMENTARIOS;

    
    
    public Colectivo(){   
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
public String GetRAMA_INDUS_INVOLUCRAD() {
        return RAMA_INDUS_INVOLUCRAD;
    }

    public void SetRAMA_INDUS_INVOLUCRAD(String RAMA_INDUS_INVOLUCRAD) {
        this.RAMA_INDUS_INVOLUCRAD = RAMA_INDUS_INVOLUCRAD;
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
public String GetDECLARACION_PERDIDA_MAY() {
        return DECLARACION_PERDIDA_MAY;
    }

    public void SetDECLARACION_PERDIDA_MAY(String DECLARACION_PERDIDA_MAY) {
        this.DECLARACION_PERDIDA_MAY = DECLARACION_PERDIDA_MAY;
    }
public String GetSUSPENSION_TMP() {
        return SUSPENSION_TMP;
    }

    public void SetSUSPENSION_TMP(String SUSPENSION_TMP) {
        this.SUSPENSION_TMP = SUSPENSION_TMP;
    }
public String GetTERMINACION_TRAB() {
        return TERMINACION_TRAB;
    }

    public void SetTERMINACION_TRAB(String TERMINACION_TRAB) {
        this.TERMINACION_TRAB = TERMINACION_TRAB;
    }
public String GetCONTRATACION_COLECTIVA() {
        return CONTRATACION_COLECTIVA;
    }

    public void SetCONTRATACION_COLECTIVA(String CONTRATACION_COLECTIVA) {
        this.CONTRATACION_COLECTIVA = CONTRATACION_COLECTIVA;
    }
public String GetOMISIONES_REGLAMENTO() {
        return OMISIONES_REGLAMENTO;
    }

    public void SetOMISIONES_REGLAMENTO(String OMISIONES_REGLAMENTO) {
        this.OMISIONES_REGLAMENTO = OMISIONES_REGLAMENTO;
    }
public String GetREDUCCION_PERSONAL() {
        return REDUCCION_PERSONAL;
    }

    public void SetREDUCCION_PERSONAL(String REDUCCION_PERSONAL) {
        this.REDUCCION_PERSONAL = REDUCCION_PERSONAL;
    }
public String GetVIOLA_DERECHOS() {
        return VIOLA_DERECHOS;
    }

    public void SetVIOLA_DERECHOS(String VIOLA_DERECHOS) {
        this.VIOLA_DERECHOS = VIOLA_DERECHOS;
    }
public String GetELECCION_SINDICALES() {
        return ELECCION_SINDICALES;
    }

    public void SetELECCION_SINDICALES(String ELECCION_SINDICALES) {
        this.ELECCION_SINDICALES = ELECCION_SINDICALES;
    }
public String GetSANCION_SINDICALES() {
        return SANCION_SINDICALES;
    }

    public void SetSANCION_SINDICALES(String SANCION_SINDICALES) {
        this.SANCION_SINDICALES = SANCION_SINDICALES;
    }
public String GetOTRO_CONFLICTO() {
        return OTRO_CONFLICTO;
    }

    public void SetOTRO_CONFLICTO(String OTRO_CONFLICTO) {
        this.OTRO_CONFLICTO = OTRO_CONFLICTO;
    }
public String GetOTRO_ESP_CONFLICTO() {
        return OTRO_ESP_CONFLICTO;
    }

    public void SetOTRO_ESP_CONFLICTO(String OTRO_ESP_CONFLICTO) {
        this.OTRO_ESP_CONFLICTO = OTRO_ESP_CONFLICTO;
    }
public String GetNO_IMPUTABLE_ST() {
        return NO_IMPUTABLE_ST;
    }

    public void SetNO_IMPUTABLE_ST(String NO_IMPUTABLE_ST) {
        this.NO_IMPUTABLE_ST = NO_IMPUTABLE_ST;
    }
public String GetINCAPACIDAD_FISICA_ST() {
        return INCAPACIDAD_FISICA_ST;
    }

    public void SetINCAPACIDAD_FISICA_ST(String INCAPACIDAD_FISICA_ST) {
        this.INCAPACIDAD_FISICA_ST = INCAPACIDAD_FISICA_ST;
    }
public String GetFALTA_MATERIA_PRIM_ST() {
        return FALTA_MATERIA_PRIM_ST;
    }

    public void SetFALTA_MATERIA_PRIM_ST(String FALTA_MATERIA_PRIM_ST) {
        this.FALTA_MATERIA_PRIM_ST = FALTA_MATERIA_PRIM_ST;
    }
public String GetFALTA_MINISTRACION_ST() {
        return FALTA_MINISTRACION_ST;
    }

    public void SetFALTA_MINISTRACION_ST(String FALTA_MINISTRACION_ST) {
        this.FALTA_MINISTRACION_ST = FALTA_MINISTRACION_ST;
    }
public String GetFUERZA_MAYOR_TC() {
        return FUERZA_MAYOR_TC;
    }

    public void SetFUERZA_MAYOR_TC(String FUERZA_MAYOR_TC) {
        this.FUERZA_MAYOR_TC = FUERZA_MAYOR_TC;
    }
public String GetINCAPACIDAD_FISICA_TC() {
        return INCAPACIDAD_FISICA_TC;
    }

    public void SetINCAPACIDAD_FISICA_TC(String INCAPACIDAD_FISICA_TC) {
        this.INCAPACIDAD_FISICA_TC = INCAPACIDAD_FISICA_TC;
    }
public String GetQUIEBRA_LEGAL_TC() {
        return QUIEBRA_LEGAL_TC;
    }

    public void SetQUIEBRA_LEGAL_TC(String QUIEBRA_LEGAL_TC) {
        this.QUIEBRA_LEGAL_TC = QUIEBRA_LEGAL_TC;
    }
public String GetAGOTAMIENTO_MATERIA_TC() {
        return AGOTAMIENTO_MATERIA_TC;
    }

    public void SetAGOTAMIENTO_MATERIA_TC(String AGOTAMIENTO_MATERIA_TC) {
        this.AGOTAMIENTO_MATERIA_TC = AGOTAMIENTO_MATERIA_TC;
    }
public String GetLIBERTAD_ASOCIACION() {
        return LIBERTAD_ASOCIACION;
    }

    public void SetLIBERTAD_ASOCIACION(String LIBERTAD_ASOCIACION) {
        this.LIBERTAD_ASOCIACION = LIBERTAD_ASOCIACION;
    }
public String GetLIBERTAD_SINDICAL() {
        return LIBERTAD_SINDICAL;
    }

    public void SetLIBERTAD_SINDICAL(String LIBERTAD_SINDICAL) {
        this.LIBERTAD_SINDICAL = LIBERTAD_SINDICAL;
    }
public String GetDERECHO_COLECTIVA() {
        return DERECHO_COLECTIVA;
    }

    public void SetDERECHO_COLECTIVA(String DERECHO_COLECTIVA) {
        this.DERECHO_COLECTIVA = DERECHO_COLECTIVA;
    }
public String GetOTRO_COLECTIVA() {
        return OTRO_COLECTIVA;
    }

    public void SetOTRO_COLECTIVA(String OTRO_COLECTIVA) {
        this.OTRO_COLECTIVA = OTRO_COLECTIVA;
    }
public String GetOTRO_ESP_COLECTIVA() {
        return OTRO_ESP_COLECTIVA;
    }

    public void SetOTRO_ESP_COLECTIVA(String OTRO_ESP_COLECTIVA) {
        this.OTRO_ESP_COLECTIVA = OTRO_ESP_COLECTIVA;
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
public String GetAUTO_DEPURACION() {
        return AUTO_DEPURACION;
    }

    public void SetAUTO_DEPURACION(String AUTO_DEPURACION) {
        this.AUTO_DEPURACION = AUTO_DEPURACION;
    }
public String GetFECHA_DEPURACION() {
        return FECHA_DEPURACION;
    }

    public void SetFECHA_DEPURACION(String FECHA_DEPURACION) {
        this.FECHA_DEPURACION = FECHA_DEPURACION;
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
public String GetFECHA_RESOLUCION_AJ() {
        return FECHA_RESOLUCION_AJ;
    }

    public void SetFECHA_RESOLUCION_AJ(String FECHA_RESOLUCION_AJ) {
        this.FECHA_RESOLUCION_AJ = FECHA_RESOLUCION_AJ;
    }
public String GetTIPO_SENTENCIA_AJ() {
        return TIPO_SENTENCIA_AJ;
    }

    public void SetTIPO_SENTENCIA_AJ(String TIPO_SENTENCIA_AJ) {
        this.TIPO_SENTENCIA_AJ = TIPO_SENTENCIA_AJ;
    }
public String GetMONTO_SOLUCION_AJ() {
        return MONTO_SOLUCION_AJ;
    }

    public void SetMONTO_SOLUCION_AJ(String MONTO_SOLUCION_AJ) {
        this.MONTO_SOLUCION_AJ = MONTO_SOLUCION_AJ;
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
         GetRAMA_INDUS_INVOLUCRAD(),
         GetSECTOR_RAMA(),
         GetSUBSECTOR_RAMA(),
         GetENTIDAD_CLAVE(),
         GetENTIDAD_NOMBRE(),
         GetMUNICIPIO_CLAVE(),
         GetMUNICIPIO_NOMBRE(),
         GetDECLARACION_PERDIDA_MAY(),
         GetSUSPENSION_TMP(),
         GetTERMINACION_TRAB(),
         GetCONTRATACION_COLECTIVA(),
         GetOMISIONES_REGLAMENTO(),
         GetREDUCCION_PERSONAL(),
         GetVIOLA_DERECHOS(),
         GetELECCION_SINDICALES(),
         GetSANCION_SINDICALES(),
         GetOTRO_CONFLICTO(),
         GetOTRO_ESP_CONFLICTO(),
         GetNO_IMPUTABLE_ST(),
         GetINCAPACIDAD_FISICA_ST(),
         GetFALTA_MATERIA_PRIM_ST(),
         GetFALTA_MINISTRACION_ST(),
         GetFUERZA_MAYOR_TC(),
         GetINCAPACIDAD_FISICA_TC(),
         GetQUIEBRA_LEGAL_TC(),
         GetAGOTAMIENTO_MATERIA_TC(),
         GetLIBERTAD_ASOCIACION(),
         GetLIBERTAD_SINDICAL(),
         GetDERECHO_COLECTIVA(),
         GetOTRO_COLECTIVA(),
         GetOTRO_ESP_COLECTIVA(),
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
         GetFECHA_ADMI_DEMANDA(),
         GetCANTIDAD_ACTORES(),
         GetCANTIDAD_DEMANDADOS(),
         GetAUTO_DEPURACION(),
         GetFECHA_DEPURACION(),
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
         GetFORMA_SOLUCION_AJ(),
         GetOTRO_ESP_SOLUCION_AJ(),
         GetFECHA_RESOLUCION_AJ(),
         GetTIPO_SENTENCIA_AJ(),
         GetMONTO_SOLUCION_AJ(),
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
