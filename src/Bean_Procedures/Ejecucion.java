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
public class Ejecucion  extends Bean implements SQLData {

    private String SQLTypeName;
    private String NOMBRE_ORGANO_JURIS;
    private String CLAVE_ORGANO;
    private String EXPEDIENTE_CLAVE;
    private String FECHA_APERTURA_EXPEDIENTE;
    private String MOTIVO_SOLICITUD_EJ;
    private String FECHA_PRESENTACION;
    private String ESTATUS_EXPE;
    private String FECHA_CONCLUSION;
    private String FASE_CONCLUSION;
    private String COMENTARIOS;





    
    
    public Ejecucion(){   
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
public String GetMOTIVO_SOLICITUD_EJ() {
        return MOTIVO_SOLICITUD_EJ;
    }

    public void SetMOTIVO_SOLICITUD_EJ(String MOTIVO_SOLICITUD_EJ) {
        this.MOTIVO_SOLICITUD_EJ = MOTIVO_SOLICITUD_EJ;
    }
public String GetFECHA_PRESENTACION() {
        return FECHA_PRESENTACION;
    }

    public void SetFECHA_PRESENTACION(String FECHA_PRESENTACION) {
        this.FECHA_PRESENTACION = FECHA_PRESENTACION;
    }
public String GetESTATUS_EXPE() {
        return ESTATUS_EXPE;
    }

    public void SetESTATUS_EXPE(String ESTATUS_EXPE) {
        this.ESTATUS_EXPE = ESTATUS_EXPE;
    }
public String GetFECHA_CONCLUSION() {
        return FECHA_CONCLUSION;
    }

    public void SetFECHA_CONCLUSION(String FECHA_CONCLUSION) {
        this.FECHA_CONCLUSION = FECHA_CONCLUSION;
    }
public String GetFASE_CONCLUSION() {
        return FASE_CONCLUSION;
    }

    public void SetFASE_CONCLUSION(String FASE_CONCLUSION) {
        this.FASE_CONCLUSION = FASE_CONCLUSION;
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
            GetMOTIVO_SOLICITUD_EJ(),
            GetFECHA_PRESENTACION(),
            GetESTATUS_EXPE(),
            GetFECHA_CONCLUSION(),
            GetFASE_CONCLUSION(),
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
