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
public class Audiencias extends Bean implements SQLData {

    private String SQLTypeName;
    private String NOMBRE_ORGANO_JURIS;
    private String CLAVE_ORGANO;
    private String EXPEDIENTE_CLAVE;
    private String TIPO_PROCED;
    private String ID_AUDIENCIA;
    private String ORDINARIO_TA;
    private String ESPECIAL_INDIVI_TA;
    private String ESPECIAL_COLECT_TA;
    private String HUELGA_TA;
    private String COL_NATU_ECONOMICA_TA;
    private String ESP_OTRO_AUDIENCIA;
    private String FECHA_AUDIEN_CELEBRADA;
    private String INICIO;
    private String CONCLU;
    private String COMENTARIOS;
    
    
    public Audiencias(){   
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
    
    public String GetEXPEDIENTE_CLAVE() {
        return EXPEDIENTE_CLAVE;
    }
     
      public void SetEXPEDIENTE_CLAVE(String EXPEDIENTE_CLAVE) {
         this.EXPEDIENTE_CLAVE=EXPEDIENTE_CLAVE;
    } 
      
   public String GetTIPO_PROCED() {
        return TIPO_PROCED;
    }
     
      public void SetTIPO_PROCED(String TIPO_PROCED) {
         this.TIPO_PROCED=TIPO_PROCED;
    } 
    
   public String GetID_AUDIENCIA() {
        return ID_AUDIENCIA;
    }
     
      public void SetID_AUDIENCIA(String ID_AUDIENCIA) {
         this.ID_AUDIENCIA=ID_AUDIENCIA;
    }    
    
     public String GetORDINARIO_TA() {
        return ORDINARIO_TA;
    }
     
      public void SetORDINARIO_TA(String ORDINARIO_TA) {
         this.ORDINARIO_TA=ORDINARIO_TA;
    } 
    
     public String GetESPECIAL_INDIVI_TA() {
        return ESPECIAL_INDIVI_TA;
    }
     
      public void SetESPECIAL_INDIVI_TA(String ESPECIAL_INDIVI_TA) {
         this.ESPECIAL_INDIVI_TA=ESPECIAL_INDIVI_TA;
    } 
    
     public String GetESPECIAL_COLECT_TA() {
        return ESPECIAL_COLECT_TA;
    }
     
      public void SetESPECIAL_COLECT_TA(String ESPECIAL_COLECT_TA) {
         this.ESPECIAL_COLECT_TA=ESPECIAL_COLECT_TA;
    } 
     
    public String GetHUELGA_TA() {
        return HUELGA_TA;
    }
     
      public void SetHUELGA_TA(String HUELGA_TA) {
         this.HUELGA_TA=HUELGA_TA;
    }   
     
    public String GetCOL_NATU_ECONOMICA_TA() {
        return COL_NATU_ECONOMICA_TA;
    }
     
      public void SetCOL_NATU_ECONOMICA_TA(String COL_NATU_ECONOMICA_TA) {
         this.COL_NATU_ECONOMICA_TA=COL_NATU_ECONOMICA_TA;
    }  
      
    public String GetESP_OTRO_AUDIENCIA() {
        return ESP_OTRO_AUDIENCIA;
    }
     
      public void SetESP_OTRO_AUDIENCIA(String ESP_OTRO_AUDIENCIA) {
         this.ESP_OTRO_AUDIENCIA=ESP_OTRO_AUDIENCIA;
    }  
    
    public String GetFECHA_AUDIEN_CELEBRADA() {
        return FECHA_AUDIEN_CELEBRADA;
    }
     
      public void SetFECHA_AUDIEN_CELEBRADA(String FECHA_AUDIEN_CELEBRADA) {
         this.FECHA_AUDIEN_CELEBRADA=FECHA_AUDIEN_CELEBRADA;
    }
     
    public String GetINICIO() {
        return INICIO;
    }
     
      public void SetINICIO(String INICIO) {
         this.INICIO=INICIO;
    }  
     
    public String GetCONCLU() {
        return CONCLU;
    }
     
      public void SetCONCLU(String CONCLU) {
         this.CONCLU=CONCLU;
    }
     
    public String GetCOMENTARIOS() {
        return COMENTARIOS;
    }
     
      public void SetCOMENTARIOS(String COMENTARIOS) {
         this.COMENTARIOS=COMENTARIOS;
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
     GetTIPO_PROCED(),
     GetID_AUDIENCIA(),
     GetORDINARIO_TA(),
     GetESPECIAL_INDIVI_TA(),
     GetESPECIAL_COLECT_TA(),
     GetHUELGA_TA(),
     GetCOL_NATU_ECONOMICA_TA(),
     GetESP_OTRO_AUDIENCIA(),
     GetFECHA_AUDIEN_CELEBRADA(),
     GetINICIO(),
     GetCONCLU(),
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
