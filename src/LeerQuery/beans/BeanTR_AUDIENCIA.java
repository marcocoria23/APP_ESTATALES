package LeerQuery.beans;

import Bean_Procedures.Bean;
import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/**
 *
 * @author ALEJANDRO.CASTILLO
 */
public class BeanTR_AUDIENCIA extends Bean implements SQLData {
      
    private String SQLTypeName;
    private String ID_AUDIENCIA;
    private String ID_ORGANOJ;
    private String FECHA_CELEBRACION;
    private String ID_TIPO_AUDIENCIA;
    private String INICIO;
    private String CONCLUSION;
    private String ID_EXPEDIENTE;
    private String COMENTARIOS;
    private String PERIODO;
    private String ID_UNIQUE;
    
    
    public String GetID_AUDIENCIA() {
        return ID_AUDIENCIA;
    }

    public void SetID_AUDIENCIA(String ID_AUDIENCIA) {
        this.ID_AUDIENCIA = ID_AUDIENCIA;
    }
    
    public String GetID_ORGANOJ() {
        return ID_ORGANOJ;
    }

    public void SetID_ORGANOJ(String ID_ORGANOJ) {
        this.ID_ORGANOJ = ID_ORGANOJ;
    }
    
    public String GetFECHA_CELEBRACION() {
        return FECHA_CELEBRACION;
    }

    public void SetFECHA_CELEBRACION(String FECHA_CELEBRACION) {
        this.FECHA_CELEBRACION = FECHA_CELEBRACION;
    }
    
    public String GetID_TIPO_AUDIENCIA() {
        return ID_TIPO_AUDIENCIA;
    }

    public void SetID_TIPO_AUDIENCIA(String ID_TIPO_AUDIENCIA) {
        this.ID_TIPO_AUDIENCIA = ID_TIPO_AUDIENCIA;
    }
    
    public String GetINICIO() {
        return INICIO;
    }

    public void SetINICIO(String INICIO) {
        this.INICIO = INICIO;
    }
    
    public String GetCONCLUSION() {
        return CONCLUSION;
    }

    public void SetCONCLUSION(String CONCLUSION) {
        this.CONCLUSION = CONCLUSION;
    }
    
    public String GetID_EXPEDIENTE() {
        return ID_EXPEDIENTE;
    }

    public void SetID_EXPEDIENTE(String ID_EXPEDIENTE) {
        this.ID_EXPEDIENTE = ID_EXPEDIENTE;
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
    
    public String GetID_UNIQUE() {
        return ID_UNIQUE;
    }

    public void SetID_UNIQUE(String ID_UNIQUE) {
        this.ID_UNIQUE = ID_UNIQUE;
    }
    
    @Override
    public Object[] getLlave() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    
    @Override
    public void setAtributos(Object[] obj) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void actualizaDesc(Connection conn) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

   
    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    
    @Override
    public String getSQLTypeName() throws SQLException {
       return SQLTypeName;
    }
    
    
      @Override
    public Object[] toArray() {
       Object[] array = {
            GetID_AUDIENCIA(),
            GetID_ORGANOJ(),
            GetFECHA_CELEBRACION(),
            GetID_TIPO_AUDIENCIA(),
            GetINICIO(),
            GetCONCLUSION(),
            GetID_EXPEDIENTE(),
            GetCOMENTARIOS(),
            GetPERIODO(),
            GetID_UNIQUE()
        };
        return array;
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
