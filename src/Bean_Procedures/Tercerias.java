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
public class Tercerias  extends Bean implements SQLData {

    private String SQLTypeName;
    private String NOMBRE_ORGANO_JURIS;
    private String CLAVE_ORGANO;
    private String EXPEDIENTE_CLAVE;
    private String CLAVE_INCIDENTAL;
    private String FECHA_INCIDENTE;
    private String FECHA_APERTURA_INCIDENTAL;
    private String TIPO_INCIDENTE;
    private String CELEBRACION_AUDIENCIA;
    private String FECHA_AUDIENCIA;
    private String ESTATUS_EXPEDIENTE;
    private String SENTENCIA_INCIDENTAL;
    private String FECHA_RESOLUCION;
    private String COMENTARIOS;
    private String PERIODO;




    
    
    public Tercerias(){   
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
public String GetCLAVE_INCIDENTAL() {
        return CLAVE_INCIDENTAL;
    }

    public void SetCLAVE_INCIDENTAL(String CLAVE_INCIDENTAL) {
        this.CLAVE_INCIDENTAL = CLAVE_INCIDENTAL;
    }
public String GetFECHA_INCIDENTE() {
        return FECHA_INCIDENTE;
    }

    public void SetFECHA_INCIDENTE(String FECHA_INCIDENTE) {
        this.FECHA_INCIDENTE = FECHA_INCIDENTE;
    }
public String GetFECHA_APERTURA_INCIDENTAL() {
        return FECHA_APERTURA_INCIDENTAL;
    }

    public void SetFECHA_APERTURA_INCIDENTAL(String FECHA_APERTURA_INCIDENTAL) {
        this.FECHA_APERTURA_INCIDENTAL = FECHA_APERTURA_INCIDENTAL;
    }
public String GetTIPO_INCIDENTE() {
        return TIPO_INCIDENTE;
    }

    public void SetTIPO_INCIDENTE(String TIPO_INCIDENTE) {
        this.TIPO_INCIDENTE = TIPO_INCIDENTE;
    }
public String GetCELEBRACION_AUDIENCIA() {
        return CELEBRACION_AUDIENCIA;
    }

    public void SetCELEBRACION_AUDIENCIA(String CELEBRACION_AUDIENCIA) {
        this.CELEBRACION_AUDIENCIA = CELEBRACION_AUDIENCIA;
    }
public String GetFECHA_AUDIENCIA() {
        return FECHA_AUDIENCIA;
    }

    public void SetFECHA_AUDIENCIA(String FECHA_AUDIENCIA) {
        this.FECHA_AUDIENCIA = FECHA_AUDIENCIA;
    }
public String GetESTATUS_EXPEDIENTE() {
        return ESTATUS_EXPEDIENTE;
    }

    public void SetESTATUS_EXPEDIENTE(String ESTATUS_EXPEDIENTE) {
        this.ESTATUS_EXPEDIENTE = ESTATUS_EXPEDIENTE;
    }
public String GetSENTENCIA_INCIDENTAL() {
        return SENTENCIA_INCIDENTAL;
    }

    public void SetSENTENCIA_INCIDENTAL(String SENTENCIA_INCIDENTAL) {
        this.SENTENCIA_INCIDENTAL = SENTENCIA_INCIDENTAL;
    }
public String GetFECHA_RESOLUCION() {
        return FECHA_RESOLUCION;
    }

    public void SetFECHA_RESOLUCION(String FECHA_RESOLUCION) {
        this.FECHA_RESOLUCION = FECHA_RESOLUCION;
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
            GetCLAVE_INCIDENTAL(),
            GetFECHA_INCIDENTE(),
            GetFECHA_APERTURA_INCIDENTAL(),
            GetTIPO_INCIDENTE(),
            GetCELEBRACION_AUDIENCIA(),
            GetFECHA_AUDIENCIA(),
            GetESTATUS_EXPEDIENTE(),
            GetSENTENCIA_INCIDENTAL(),
            GetFECHA_RESOLUCION(),
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
