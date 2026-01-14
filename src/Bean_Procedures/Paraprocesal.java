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
public class Paraprocesal  extends Bean implements SQLData {

    private String SQLTypeName;
    private String NOMBRE_ORGANO_JURIS;
    private String CLAVE_ORGANO;
    private String EXPEDIENTE_CLAVE;
    private String FECHA_APERTURA_EXPEDIENTE;
    private String RAMA_INVOLUCRAD;
    private String SECTOR_RAMA;
    private String SUBSECTOR_RAMA;
    private String MOTIVO_SOLICITUD;
    private String ESPECIFIQUE_MOTIVO;
    private String INCOMPETENCIA;
    private String TIPO_INCOMPETENCIA;
    private String ESPECIFIQUE_INCOMP;
    private String FECHA_PRESENTA_SOLI;
    private String FECHA_ADMISION_SOLI;
    private String PROMOVENTE;
    private String ESPECIFIQUE_PROMOVENTE;
    private String ESTATUS_EXPEDIENTE;
    private String FECHA_CONCLUSION_EXPE;
    private String COMENTARIOS;



    
    
    public Paraprocesal(){   
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
public String GetMOTIVO_SOLICITUD() {
        return MOTIVO_SOLICITUD;
    }

    public void SetMOTIVO_SOLICITUD(String MOTIVO_SOLICITUD) {
        this.MOTIVO_SOLICITUD = MOTIVO_SOLICITUD;
    }
public String GetESPECIFIQUE_MOTIVO() {
        return ESPECIFIQUE_MOTIVO;
    }

    public void SetESPECIFIQUE_MOTIVO(String ESPECIFIQUE_MOTIVO) {
        this.ESPECIFIQUE_MOTIVO = ESPECIFIQUE_MOTIVO;
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
public String GetFECHA_PRESENTA_SOLI() {
        return FECHA_PRESENTA_SOLI;
    }

    public void SetFECHA_PRESENTA_SOLI(String FECHA_PRESENTA_SOLI) {
        this.FECHA_PRESENTA_SOLI = FECHA_PRESENTA_SOLI;
    }
public String GetFECHA_ADMISION_SOLI() {
        return FECHA_ADMISION_SOLI;
    }

    public void SetFECHA_ADMISION_SOLI(String FECHA_ADMISION_SOLI) {
        this.FECHA_ADMISION_SOLI = FECHA_ADMISION_SOLI;
    }
public String GetPROMOVENTE() {
        return PROMOVENTE;
    }

    public void SetPROMOVENTE(String PROMOVENTE) {
        this.PROMOVENTE = PROMOVENTE;
    }
public String GetESPECIFIQUE_PROMOVENTE() {
        return ESPECIFIQUE_PROMOVENTE;
    }

    public void SetESPECIFIQUE_PROMOVENTE(String ESPECIFIQUE_PROMOVENTE) {
        this.ESPECIFIQUE_PROMOVENTE = ESPECIFIQUE_PROMOVENTE;
    }
public String GetESTATUS_EXPEDIENTE() {
        return ESTATUS_EXPEDIENTE;
    }

    public void SetESTATUS_EXPEDIENTE(String ESTATUS_EXPEDIENTE) {
        this.ESTATUS_EXPEDIENTE = ESTATUS_EXPEDIENTE;
    }
public String GetFECHA_CONCLUSION_EXPE() {
        return FECHA_CONCLUSION_EXPE;
    }

    public void SetFECHA_CONCLUSION_EXPE(String FECHA_CONCLUSION_EXPE) {
        this.FECHA_CONCLUSION_EXPE = FECHA_CONCLUSION_EXPE;
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
         GetRAMA_INVOLUCRAD(),
         GetSECTOR_RAMA(),
         GetSUBSECTOR_RAMA(),
         GetMOTIVO_SOLICITUD(),
         GetESPECIFIQUE_MOTIVO(),
         GetINCOMPETENCIA(),
         GetTIPO_INCOMPETENCIA(),
         GetESPECIFIQUE_INCOMP(),
         GetFECHA_PRESENTA_SOLI(),
         GetFECHA_ADMISION_SOLI(),
         GetPROMOVENTE(),
         GetESPECIFIQUE_PROMOVENTE(),
         GetESTATUS_EXPEDIENTE(),
         GetFECHA_CONCLUSION_EXPE(),
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
