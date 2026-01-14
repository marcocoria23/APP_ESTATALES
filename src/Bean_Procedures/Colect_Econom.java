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
public class Colect_Econom extends Bean implements SQLData {

    private String SQLTypeName;
    private String NOMBRE_ORGANO_JURIS;
    private String CLAVE_ORGANO;
    private String EXPEDIENTE_CLAVE;
    private String FECHA_APERTURA_EXPEDIENTE;
    private String TIPO_ASUNTO;
    private String NAT_CONFLICTO;
    private String RAMA_INVOLUCRAD;
    private String SECTOR_RAMA;
    private String SUBSECTOR_RAMA;
    private String ENTIDAD_CLAVE;
    private String ENTIDAD_NOMBRE;
    private String MUNICIPIO_CLAVE;
    private String MUNICIPIO_NOMBRE;
    private String MODIF_CONDICIONES;
    private String NUEVAS_CONDICIONES;
    private String SUSPENSION_TEMPORAL;
    private String TERMINACION_COLECTIVA;
    private String OTRO_MOTIVO_ECONOM;
    private String ESPECIFIQUE_ECONOM;
    private String EXCESO_PRODUCCION;
    private String INCOSTEABILIDAD;
    private String FALTA_FONDOS;
    private String INCOMPETENCIA;
    private String TIPO_INCOMPETENCIA;
    private String ESPECIFIQUE_INCOMP;
    private String FECHA_PRES_DEMANDA;
    private String CONSTANCIA_CONS_EXPEDIDA;
    private String CONSTANCIA_CLAVE;
    private String ASUN_EXCEP_CONCILIACION;
    private String PREVE_DEMANDA;
    private String DESAHOGO_PREV_DEMANDA;
    private String ESTATUS_DEMANDA;
    private String FECHA_ADMISION_DEMANDA;
    private String CANTIDAD_ACTORES;
    private String CANTIDAD_DEMANDADOS;
    private String AUDIENCIA_ECONOM;
    private String FECHA_AUDIENCIA_ECONOM;
    private String ESTATUS_EXPEDIENTE;
    private String FECHA_ACTO_PROCESAL;
    private String FASE_SOLI_EXPEDIENTE;
    private String FORMA_SOLUCION;
    private String ESPECIFIQUE_FORMA;
    private String FECHA_RESOLUCION;
    private String TIPO_SENTENCIA;
    private String AUMENTO_PERSONAL;
    private String DISMINUCION_PERSONAL;
    private String AUMENTO_JORNADALAB;
    private String DISMINUCION_JORNADALAB;
    private String AUMENTO_SEMANA;
    private String DISMINUCION_SEMANA;
    private String AUMENTO_SALARIOS;
    private String DISMINUCION_SALARIOS;
    private String OTRO_TIPO;
    private String ESPECIFIQUE_TIPO;
    private String COMENTARIOS;

    
    
    public Colect_Econom(){   
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
public String GetRAMA_INVOLUCRAD() {
        return RAMA_INVOLUCRAD;
    }

    public void SetRAMA_INVOLUCRAD(String RAMA_INVOLUCRAD) {
        this.RAMA_INVOLUCRAD = RAMA_INVOLUCRAD;
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
public String GetMODIF_CONDICIONES() {
        return MODIF_CONDICIONES;
    }

    public void SetMODIF_CONDICIONES(String MODIF_CONDICIONES) {
        this.MODIF_CONDICIONES = MODIF_CONDICIONES;
    }
public String GetNUEVAS_CONDICIONES() {
        return NUEVAS_CONDICIONES;
    }

    public void SetNUEVAS_CONDICIONES(String NUEVAS_CONDICIONES) {
        this.NUEVAS_CONDICIONES = NUEVAS_CONDICIONES;
    }
public String GetSUSPENSION_TEMPORAL() {
        return SUSPENSION_TEMPORAL;
    }

    public void SetSUSPENSION_TEMPORAL(String SUSPENSION_TEMPORAL) {
        this.SUSPENSION_TEMPORAL = SUSPENSION_TEMPORAL;
    }
public String GetTERMINACION_COLECTIVA() {
        return TERMINACION_COLECTIVA;
    }

    public void SetTERMINACION_COLECTIVA(String TERMINACION_COLECTIVA) {
        this.TERMINACION_COLECTIVA = TERMINACION_COLECTIVA;
    }
public String GetOTRO_MOTIVO_ECONOM() {
        return OTRO_MOTIVO_ECONOM;
    }

    public void SetOTRO_MOTIVO_ECONOM(String OTRO_MOTIVO_ECONOM) {
        this.OTRO_MOTIVO_ECONOM = OTRO_MOTIVO_ECONOM;
    }
public String GetESPECIFIQUE_ECONOM() {
        return ESPECIFIQUE_ECONOM;
    }

    public void SetESPECIFIQUE_ECONOM(String ESPECIFIQUE_ECONOM) {
        this.ESPECIFIQUE_ECONOM = ESPECIFIQUE_ECONOM;
    }
public String GetEXCESO_PRODUCCION() {
        return EXCESO_PRODUCCION;
    }

    public void SetEXCESO_PRODUCCION(String EXCESO_PRODUCCION) {
        this.EXCESO_PRODUCCION = EXCESO_PRODUCCION;
    }
public String GetINCOSTEABILIDAD() {
        return INCOSTEABILIDAD;
    }

    public void SetINCOSTEABILIDAD(String INCOSTEABILIDAD) {
        this.INCOSTEABILIDAD = INCOSTEABILIDAD;
    }
public String GetFALTA_FONDOS() {
        return FALTA_FONDOS;
    }

    public void SetFALTA_FONDOS(String FALTA_FONDOS) {
        this.FALTA_FONDOS = FALTA_FONDOS;
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
public String GetESPECIFIQUE_INCOMP() {
        return ESPECIFIQUE_INCOMP;
    }

    public void SetESPECIFIQUE_INCOMP(String ESPECIFIQUE_INCOMP) {
        this.ESPECIFIQUE_INCOMP = ESPECIFIQUE_INCOMP;
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
public String GetFECHA_ADMISION_DEMANDA() {
        return FECHA_ADMISION_DEMANDA;
    }

    public void SetFECHA_ADMISION_DEMANDA(String FECHA_ADMISION_DEMANDA) {
        this.FECHA_ADMISION_DEMANDA = FECHA_ADMISION_DEMANDA;
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
public String GetAUDIENCIA_ECONOM() {
        return AUDIENCIA_ECONOM;
    }

    public void SetAUDIENCIA_ECONOM(String AUDIENCIA_ECONOM) {
        this.AUDIENCIA_ECONOM = AUDIENCIA_ECONOM;
    }
public String GetFECHA_AUDIENCIA_ECONOM() {
        return FECHA_AUDIENCIA_ECONOM;
    }

    public void SetFECHA_AUDIENCIA_ECONOM(String FECHA_AUDIENCIA_ECONOM) {
        this.FECHA_AUDIENCIA_ECONOM = FECHA_AUDIENCIA_ECONOM;
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
public String GetFORMA_SOLUCION() {
        return FORMA_SOLUCION;
    }

    public void SetFORMA_SOLUCION(String FORMA_SOLUCION) {
        this.FORMA_SOLUCION = FORMA_SOLUCION;
    }
public String GetESPECIFIQUE_FORMA() {
        return ESPECIFIQUE_FORMA;
    }

    public void SetESPECIFIQUE_FORMA(String ESPECIFIQUE_FORMA) {
        this.ESPECIFIQUE_FORMA = ESPECIFIQUE_FORMA;
    }
public String GetFECHA_RESOLUCION() {
        return FECHA_RESOLUCION;
    }

    public void SetFECHA_RESOLUCION(String FECHA_RESOLUCION) {
        this.FECHA_RESOLUCION = FECHA_RESOLUCION;
    }
public String GetTIPO_SENTENCIA() {
        return TIPO_SENTENCIA;
    }

    public void SetTIPO_SENTENCIA(String TIPO_SENTENCIA) {
        this.TIPO_SENTENCIA = TIPO_SENTENCIA;
    }
public String GetAUMENTO_PERSONAL() {
        return AUMENTO_PERSONAL;
    }

    public void SetAUMENTO_PERSONAL(String AUMENTO_PERSONAL) {
        this.AUMENTO_PERSONAL = AUMENTO_PERSONAL;
    }
public String GetDISMINUCION_PERSONAL() {
        return DISMINUCION_PERSONAL;
    }

    public void SetDISMINUCION_PERSONAL(String DISMINUCION_PERSONAL) {
        this.DISMINUCION_PERSONAL = DISMINUCION_PERSONAL;
    }
public String GetAUMENTO_JORNADALAB() {
        return AUMENTO_JORNADALAB;
    }

    public void SetAUMENTO_JORNADALAB(String AUMENTO_JORNADALAB) {
        this.AUMENTO_JORNADALAB = AUMENTO_JORNADALAB;
    }
public String GetDISMINUCION_JORNADALAB() {
        return DISMINUCION_JORNADALAB;
    }

    public void SetDISMINUCION_JORNADALAB(String DISMINUCION_JORNADALAB) {
        this.DISMINUCION_JORNADALAB = DISMINUCION_JORNADALAB;
    }
public String GetAUMENTO_SEMANA() {
        return AUMENTO_SEMANA;
    }

    public void SetAUMENTO_SEMANA(String AUMENTO_SEMANA) {
        this.AUMENTO_SEMANA = AUMENTO_SEMANA;
    }
public String GetDISMINUCION_SEMANA() {
        return DISMINUCION_SEMANA;
    }

    public void SetDISMINUCION_SEMANA(String DISMINUCION_SEMANA) {
        this.DISMINUCION_SEMANA = DISMINUCION_SEMANA;
    }
public String GetAUMENTO_SALARIOS() {
        return AUMENTO_SALARIOS;
    }

    public void SetAUMENTO_SALARIOS(String AUMENTO_SALARIOS) {
        this.AUMENTO_SALARIOS = AUMENTO_SALARIOS;
    }
public String GetDISMINUCION_SALARIOS() {
        return DISMINUCION_SALARIOS;
    }

    public void SetDISMINUCION_SALARIOS(String DISMINUCION_SALARIOS) {
        this.DISMINUCION_SALARIOS = DISMINUCION_SALARIOS;
    }
public String GetOTRO_TIPO() {
        return OTRO_TIPO;
    }

    public void SetOTRO_TIPO(String OTRO_TIPO) {
        this.OTRO_TIPO = OTRO_TIPO;
    }
public String GetESPECIFIQUE_TIPO() {
        return ESPECIFIQUE_TIPO;
    }

    public void SetESPECIFIQUE_TIPO(String ESPECIFIQUE_TIPO) {
        this.ESPECIFIQUE_TIPO = ESPECIFIQUE_TIPO;
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
         GetRAMA_INVOLUCRAD(),
         GetSECTOR_RAMA(),
         GetSUBSECTOR_RAMA(),
         GetENTIDAD_CLAVE(),
         GetENTIDAD_NOMBRE(),
         GetMUNICIPIO_CLAVE(),
         GetMUNICIPIO_NOMBRE(),
         GetMODIF_CONDICIONES(),
         GetNUEVAS_CONDICIONES(),
         GetSUSPENSION_TEMPORAL(),
         GetTERMINACION_COLECTIVA(),
         GetOTRO_MOTIVO_ECONOM(),
         GetESPECIFIQUE_ECONOM(),
         GetEXCESO_PRODUCCION(),
         GetINCOSTEABILIDAD(),
         GetFALTA_FONDOS(),
         GetINCOMPETENCIA(),
         GetTIPO_INCOMPETENCIA(),
         GetESPECIFIQUE_INCOMP(),
         GetFECHA_PRES_DEMANDA(),
         GetCONSTANCIA_CONS_EXPEDIDA(),
         GetCONSTANCIA_CLAVE(),
         GetASUN_EXCEP_CONCILIACION(),
         GetPREVE_DEMANDA(),
         GetDESAHOGO_PREV_DEMANDA(),
         GetESTATUS_DEMANDA(),
         GetFECHA_ADMISION_DEMANDA(),
         GetCANTIDAD_ACTORES(),
         GetCANTIDAD_DEMANDADOS(),
         GetAUDIENCIA_ECONOM(),
         GetFECHA_AUDIENCIA_ECONOM(),
         GetESTATUS_EXPEDIENTE(),
         GetFECHA_ACTO_PROCESAL(),
         GetFASE_SOLI_EXPEDIENTE(),
         GetFORMA_SOLUCION(),
         GetESPECIFIQUE_FORMA(),
         GetFECHA_RESOLUCION(),
         GetTIPO_SENTENCIA(),
         GetAUMENTO_PERSONAL(),
         GetDISMINUCION_PERSONAL(),
         GetAUMENTO_JORNADALAB(),
         GetDISMINUCION_JORNADALAB(),
         GetAUMENTO_SEMANA(),
         GetDISMINUCION_SEMANA(),
         GetAUMENTO_SALARIOS(),
         GetDISMINUCION_SALARIOS(),
         GetOTRO_TIPO(),
         GetESPECIFIQUE_TIPO(),
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

