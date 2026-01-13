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
public class ControlExpediente  extends Bean implements SQLData {

    private String SQLTypeName;
    private String ID_CONTROL;
    private String NOMBRE_ORGANO_JURIS;
    private String CLAVE_ORGANO;
    private String SEDE;
    private String JUECES_LABORAL_TOTAL;
    private String JUECES_LABORAL_SUB_HOM;
    private String JUECES_LABORAL_SUB_MUJ;
    private String JUECES_LABORAL_INDIV_HOM;
    private String JUECES_LABORAL_INDIV_MUJ;
    private String JUECES_LABORAL_COLEC_HOM;
    private String JUECES_LABORAL_COLEC_MUJ;
    private String JUECES_LABORAL_MIX_HOM;
    private String JUECES_LABORAL_MIX_MUJ;
    private String HORARIO;
    private String ENTIDAD_CLAVE;
    private String ENTIDAD_NOMBRE;
    private String MUNICIPIO_CLAVE;
    private String MUNICIPIO_NOMBRE;
    private String COLONIA_NOMBRE;
    private String LATITUD_ORG1;
    private String LONGITUD_ORG1;
    private String CIRCUNS_ORG_JUR;
    private String OTRO_ESP_CIRCUNS;
    private String JURISDICCION;
    private String ORDINARIO;
    private String ESPECIAL_INDIVI;
    private String ESPECIAL_COLECT;
    private String HUELGA;
    private String COL_NATU_ECONOMICA;
    private String PARAP_VOLUNTARIO;
    private String TERCERIAS;
    private String PREF_CREDITO;
    private String EJECUCION;
    private String PERIODO;
    private String ESTATUS;
    private String VERSIONES;
  
    
    public ControlExpediente(){   
    }
    
      public String GetID_CONTROL() {
        return ID_CONTROL;
    }
     
      public void SetID_CONTROL(String ID_CONTROL) {
         this.ID_CONTROL=ID_CONTROL;
    }
    
     public String GetNOMBRE_ORGANO_JURIS() {
        return NOMBRE_ORGANO_JURIS;
    }
     
      public void SetNOMBRE_ORGANO_JURIS(String NOMBRE_ORGANO_JURIS) {
         this.NOMBRE_ORGANO_JURIS=NOMBRE_ORGANO_JURIS;
    }
     
   public String GetCLAVE_ORGANO() {
        return CLAVE_ORGANO;
    }
     
      public void SetCLAVE_ORGANO(String CLAVE_ORGANO) {
         this.CLAVE_ORGANO=CLAVE_ORGANO;
    }   
   
    public String GetSEDE() {
        return SEDE;
    }
     
      public void SetSEDE(String SEDE) {
         this.SEDE=SEDE;
    }         
      
     public String GetJUECES_LABORAL_TOTAL() {
        return JUECES_LABORAL_TOTAL;
    }
     
      public void SetJUECES_LABORAL_TOTAL(String JUECES_LABORAL_TOTAL) {
         this.JUECES_LABORAL_TOTAL=JUECES_LABORAL_TOTAL;
    }   
     
      public String GetJUECES_LABORAL_SUB_HOM() {
        return JUECES_LABORAL_SUB_HOM;
    }
     
      public void SetJUECES_LABORAL_SUB_HOM(String JUECES_LABORAL_SUB_HOM) {
         this.JUECES_LABORAL_SUB_HOM=JUECES_LABORAL_SUB_HOM;
    }   
      
    public String GetJUECES_LABORAL_SUB_MUJ() {
        return JUECES_LABORAL_SUB_MUJ;
    }
     
      public void SetJUECES_LABORAL_SUB_MUJ(String JUECES_LABORAL_SUB_MUJ) {
         this.JUECES_LABORAL_SUB_MUJ=JUECES_LABORAL_SUB_MUJ;
    }
     
      public String GetLATITUD_ORG1() {
        return LATITUD_ORG1;
    }
     
      public void SetLATITUD_ORG1(String LATITUD_ORG1) {
         this.LATITUD_ORG1=LATITUD_ORG1;
    }   
      
    
      
   public String GetJUECES_LABORAL_INDIV_HOM() {
        return JUECES_LABORAL_INDIV_HOM;
    }
     
      public void SetJUECES_LABORAL_INDIV_HOM(String JUECES_LABORAL_INDIV_HOM) {
         this.JUECES_LABORAL_INDIV_HOM=JUECES_LABORAL_INDIV_HOM;
    }   
    
      public String GetJUECES_LABORAL_INDIV_MUJ() {
        return JUECES_LABORAL_INDIV_MUJ;
    }
     
      public void SetJUECES_LABORAL_INDIV_MUJ(String JUECES_LABORAL_INDIV_MUJ) {
         this.JUECES_LABORAL_INDIV_MUJ=JUECES_LABORAL_INDIV_MUJ;
    }   
      
    public String GetJUECES_LABORAL_COLEC_HOM() {
        return JUECES_LABORAL_COLEC_HOM;
    }
     
      public void SetJUECES_LABORAL_COLEC_HOM(String JUECES_LABORAL_COLEC_HOM) {
         this.JUECES_LABORAL_COLEC_HOM=JUECES_LABORAL_COLEC_HOM;
    }
      
    public String GetJUECES_LABORAL_COLEC_MUJ() {
        return JUECES_LABORAL_COLEC_MUJ;
    }
     
      public void SetJUECES_LABORAL_COLEC_MUJ(String JUECES_LABORAL_COLEC_MUJ) {
         this.JUECES_LABORAL_COLEC_MUJ=JUECES_LABORAL_COLEC_MUJ;
    }
      
    public String GetJUECES_LABORAL_MIX_HOM() {
        return JUECES_LABORAL_MIX_HOM;
    }
     
      public void SetJUECES_LABORAL_MIX_HOM(String JUECES_LABORAL_MIX_HOM) {
         this.JUECES_LABORAL_MIX_HOM=JUECES_LABORAL_MIX_HOM;
    }  
      
    public String GetJUECES_LABORAL_MIX_MUJ() {
        return JUECES_LABORAL_MIX_MUJ;
    }
     
      public void SetJUECES_LABORAL_MIX_MUJ(String JUECES_LABORAL_MIX_MUJ) {
         this.JUECES_LABORAL_MIX_MUJ=JUECES_LABORAL_MIX_MUJ;
    }
      
    public String GetHORARIO() {
        return HORARIO;
    }
     
      public void SetHORARIO(String HORARIO) {
         this.HORARIO=HORARIO;
    }     
    
   public String GetENTIDAD_CLAVE() {
        return ENTIDAD_CLAVE;
    }
     
      public void SetENTIDAD_CLAVE(String ENTIDAD_CLAVE) {
         this.ENTIDAD_CLAVE=ENTIDAD_CLAVE;
    }   
      
    public String GetENTIDAD_NOMBRE() {
        return ENTIDAD_NOMBRE;
    }
     
      public void SetENTIDAD_NOMBRE(String ENTIDAD_NOMBRE) {
         this.ENTIDAD_NOMBRE=ENTIDAD_NOMBRE;
    }     
      
     public String GetMUNICIPIO_CLAVE() {
        return MUNICIPIO_CLAVE;
    }
     
      public void SetMUNICIPIO_CLAVE(String MUNICIPIO_CLAVE) {
         this.MUNICIPIO_CLAVE=MUNICIPIO_CLAVE;
    }    
      
     public String GetMUNICIPIO_NOMBRE() {
        return MUNICIPIO_NOMBRE;
    }
     
      public void SetMUNICIPIO_NOMBRE(String MUNICIPIO_NOMBRE) {
         this.MUNICIPIO_NOMBRE=MUNICIPIO_NOMBRE;
    }    
     
      
      public String GetCOLONIA_NOMBRE() {
        return COLONIA_NOMBRE;
    }
     
      public void SetCOLONIA_NOMBRE(String COLONIA_NOMBRE) {
         this.COLONIA_NOMBRE=COLONIA_NOMBRE;
    }   
      
    public String GetLONGITUD_ORG1() {
        return LONGITUD_ORG1;
    }
     
      public void SetLONGITUD_ORG1(String LONGITUD_ORG1) {
         this.LONGITUD_ORG1=LONGITUD_ORG1;
    }     
      
     public String GetCIRCUNS_ORG_JUR() {
        return CIRCUNS_ORG_JUR;
    }
     
      public void SetCIRCUNS_ORG_JUR(String CIRCUNS_ORG_JUR) {
         this.CIRCUNS_ORG_JUR=CIRCUNS_ORG_JUR;
    }    
   
      
      public String GetOTRO_ESP_CIRCUNS() {
        return OTRO_ESP_CIRCUNS;
    }
     
      public void SetOTRO_ESP_CIRCUNS(String OTRO_ESP_CIRCUNS) {
         this.OTRO_ESP_CIRCUNS=OTRO_ESP_CIRCUNS;
    }   
      
      
   public String GetJURISDICCION() {
        return JURISDICCION;
    }
     
      public void SetJURISDICCION(String JURISDICCION) {
         this.JURISDICCION=JURISDICCION;
    }   
      
      public String GetORDINARIO() {
        return ORDINARIO;
    }
     
      public void SetORDINARIO(String ORDINARIO) {
         this.ORDINARIO=ORDINARIO;
    } 
      
      
      public String GetESPECIAL_INDIVI() {
        return ESPECIAL_INDIVI;
    }
     
      public void SetESPECIAL_INDIVI(String ESPECIAL_INDIVI) {
         this.ESPECIAL_INDIVI=ESPECIAL_INDIVI;
    }   
      
     public String GetESPECIAL_COLECT() {
        return ESPECIAL_COLECT;
    }
     
      public void SetESPECIAL_COLECT(String ESPECIAL_COLECT) {
         this.ESPECIAL_COLECT=ESPECIAL_COLECT;
    }    
      
      public String GetHUELGA() {
        return HUELGA;
    }
     
      public void SetHUELGA(String HUELGA) {
         this.HUELGA=HUELGA;
    }  
      
      public String GetCOL_NATU_ECONOMICA() {
        return COL_NATU_ECONOMICA;
    }
     
      public void SetCOL_NATU_ECONOMICA(String COL_NATU_ECONOMICA) {
         this.COL_NATU_ECONOMICA=COL_NATU_ECONOMICA;
    }   
      
    public String GetPARAP_VOLUNTARIO() {
        return PARAP_VOLUNTARIO;
    }
     
      public void SetPARAP_VOLUNTARIO(String PARAP_VOLUNTARIO) {
         this.PARAP_VOLUNTARIO=PARAP_VOLUNTARIO;
    }   
      
   public String GetTERCERIAS() {
        return TERCERIAS;
    }
     
      public void SetTERCERIAS(String TERCERIAS) {
         this.TERCERIAS=TERCERIAS;
    }  
      
    public String GetPREF_CREDITO() {
        return PREF_CREDITO;
    }
     
      public void SetPREF_CREDITO(String PREF_CREDITO) {
         this.PREF_CREDITO=PREF_CREDITO;
    }     
      
   public String GetEJECUCION() {
        return EJECUCION;
    }
     
      public void SetEJECUCION(String EJECUCION) {
         this.EJECUCION=EJECUCION;
    }      
      
      
   public String GetPERIODO() {
        return PERIODO;
    }
     
      public void SetPERIODO(String PERIODO) {
         this.PERIODO=PERIODO;
    }   
      
    public String GetESTATUS() {
        return ESTATUS;
    }
     
      public void SetESTATUS(String ESTATUS) {
         this.ESTATUS=ESTATUS;
    }   
      
   public String GetVERSIONES() {
        return VERSIONES;
    }
     
      public void SetVERSIONES(String VERSIONES) {
         this.VERSIONES=VERSIONES;
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
            GetID_CONTROL(),
            GetNOMBRE_ORGANO_JURIS(),
            GetCLAVE_ORGANO(),
            GetSEDE(),
            GetJUECES_LABORAL_TOTAL(),
            GetJUECES_LABORAL_SUB_HOM(),
            GetJUECES_LABORAL_SUB_MUJ(),
            GetJUECES_LABORAL_INDIV_HOM(),
            GetJUECES_LABORAL_INDIV_MUJ(),
            GetJUECES_LABORAL_COLEC_HOM(),
            GetJUECES_LABORAL_COLEC_MUJ(),
            GetJUECES_LABORAL_MIX_HOM(),
            GetJUECES_LABORAL_MIX_MUJ(),
            GetHORARIO(),
            GetENTIDAD_CLAVE(),
            GetENTIDAD_NOMBRE(),
            GetMUNICIPIO_CLAVE(),
            GetMUNICIPIO_NOMBRE(),
            GetCOLONIA_NOMBRE(),
            GetLATITUD_ORG1(),
            GetLONGITUD_ORG1(),
            GetCIRCUNS_ORG_JUR(),
            GetOTRO_ESP_CIRCUNS(),
            GetJURISDICCION(),
            GetORDINARIO(),
            GetESPECIAL_INDIVI(),
            GetESPECIAL_COLECT(),
            GetHUELGA(),
            GetCOL_NATU_ECONOMICA(),
            GetPARAP_VOLUNTARIO(),
            GetTERCERIAS(),
            GetPREF_CREDITO(),
            GetEJECUCION(),
            GetPERIODO(),
            GetESTATUS(),
            GetVERSIONES()
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
