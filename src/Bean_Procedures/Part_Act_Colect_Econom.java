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
public class Part_Act_Colect_Econom  extends Bean implements SQLData {

    private String SQLTypeName;
    private String NOMBRE_ORGANO_JURIS;
    private String CLAVE_ORGANO;
    private String EXPEDIENTE_CLAVE;
    private String ID_ACTOR;
    private String ACTOR;
    private String DEFENSA_ACT;
    private String NOMBRE_SINDICATO;
    private String REG_ASOC_SINDICAL;
    private String TIPO_SINDICATO;
    private String OTRO_ESP_SINDICATO;
    private String ORG_OBRERA;
    private String NOMBRE_ORG_OBRERA;
    private String OTRO_ESP_OBRERA;
    private String CANT_TRABAJA_INV;
    private String HOMBRES;
    private String MUJERES;
    private String NO_IDENTIFICADO;
    private String TIPO;
    private String RFC_PATRON;
    private String RAZON_SOCIAL_EMPR;
    private String CALLE;
    private String N_EXT;
    private String N_INT;
    private String COLONIA;
    private String CP;
    private String ENTIDAD_NOMBRE_EMPR;
    private String ENTIDAD_CLAVE_EMPR;
    private String MUNICIPIO_NOMBRE_EMPR;
    private String MUNICIPIO_CLAVE_EMPR;
    private String LATITUD_EMPR1;
    private String LONGITUD_EMPR1;
    private String COMENTARIOS;

    
    
    public Part_Act_Colect_Econom(){   
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
public String GetID_ACTOR() {
        return ID_ACTOR;
    }

    public void SetID_ACTOR(String ID_ACTOR) {
        this.ID_ACTOR = ID_ACTOR;
    }
public String GetACTOR() {
        return ACTOR;
    }

    public void SetACTOR(String ACTOR) {
        this.ACTOR = ACTOR;
    }
public String GetDEFENSA_ACT() {
        return DEFENSA_ACT;
    }

    public void SetDEFENSA_ACT(String DEFENSA_ACT) {
        this.DEFENSA_ACT = DEFENSA_ACT;
    }
public String GetNOMBRE_SINDICATO() {
        return NOMBRE_SINDICATO;
    }

    public void SetNOMBRE_SINDICATO(String NOMBRE_SINDICATO) {
        this.NOMBRE_SINDICATO = NOMBRE_SINDICATO;
    }
public String GetREG_ASOC_SINDICAL() {
        return REG_ASOC_SINDICAL;
    }

    public void SetREG_ASOC_SINDICAL(String REG_ASOC_SINDICAL) {
        this.REG_ASOC_SINDICAL = REG_ASOC_SINDICAL;
    }
public String GetTIPO_SINDICATO() {
        return TIPO_SINDICATO;
    }

    public void SetTIPO_SINDICATO(String TIPO_SINDICATO) {
        this.TIPO_SINDICATO = TIPO_SINDICATO;
    }
public String GetOTRO_ESP_SINDICATO() {
        return OTRO_ESP_SINDICATO;
    }

    public void SetOTRO_ESP_SINDICATO(String OTRO_ESP_SINDICATO) {
        this.OTRO_ESP_SINDICATO = OTRO_ESP_SINDICATO;
    }
public String GetORG_OBRERA() {
        return ORG_OBRERA;
    }

    public void SetORG_OBRERA(String ORG_OBRERA) {
        this.ORG_OBRERA = ORG_OBRERA;
    }
public String GetNOMBRE_ORG_OBRERA() {
        return NOMBRE_ORG_OBRERA;
    }

    public void SetNOMBRE_ORG_OBRERA(String NOMBRE_ORG_OBRERA) {
        this.NOMBRE_ORG_OBRERA = NOMBRE_ORG_OBRERA;
    }
public String GetOTRO_ESP_OBRERA() {
        return OTRO_ESP_OBRERA;
    }

    public void SetOTRO_ESP_OBRERA(String OTRO_ESP_OBRERA) {
        this.OTRO_ESP_OBRERA = OTRO_ESP_OBRERA;
    }
public String GetCANT_TRABAJA_INV() {
        return CANT_TRABAJA_INV;
    }

    public void SetCANT_TRABAJA_INV(String CANT_TRABAJA_INV) {
        this.CANT_TRABAJA_INV = CANT_TRABAJA_INV;
    }
public String GetHOMBRES() {
        return HOMBRES;
    }

    public void SetHOMBRES(String HOMBRES) {
        this.HOMBRES = HOMBRES;
    }
public String GetMUJERES() {
        return MUJERES;
    }

    public void SetMUJERES(String MUJERES) {
        this.MUJERES = MUJERES;
    }
public String GetNO_IDENTIFICADO() {
        return NO_IDENTIFICADO;
    }

    public void SetNO_IDENTIFICADO(String NO_IDENTIFICADO) {
        this.NO_IDENTIFICADO = NO_IDENTIFICADO;
    }
public String GetTIPO() {
        return TIPO;
    }

    public void SetTIPO(String TIPO) {
        this.TIPO = TIPO;
    }
public String GetRFC_PATRON() {
        return RFC_PATRON;
    }

    public void SetRFC_PATRON(String RFC_PATRON) {
        this.RFC_PATRON = RFC_PATRON;
    }
public String GetRAZON_SOCIAL_EMPR() {
        return RAZON_SOCIAL_EMPR;
    }

    public void SetRAZON_SOCIAL_EMPR(String RAZON_SOCIAL_EMPR) {
        this.RAZON_SOCIAL_EMPR = RAZON_SOCIAL_EMPR;
    }
public String GetCALLE() {
        return CALLE;
    }

    public void SetCALLE(String CALLE) {
        this.CALLE = CALLE;
    }
public String GetN_EXT() {
        return N_EXT;
    }

    public void SetN_EXT(String N_EXT) {
        this.N_EXT = N_EXT;
    }
public String GetN_INT() {
        return N_INT;
    }

    public void SetN_INT(String N_INT) {
        this.N_INT = N_INT;
    }
public String GetCOLONIA() {
        return COLONIA;
    }

    public void SetCOLONIA(String COLONIA) {
        this.COLONIA = COLONIA;
    }
public String GetCP() {
        return CP;
    }

    public void SetCP(String CP) {
        this.CP = CP;
    }
public String GetENTIDAD_NOMBRE_EMPR() {
        return ENTIDAD_NOMBRE_EMPR;
    }

    public void SetENTIDAD_NOMBRE_EMPR(String ENTIDAD_NOMBRE_EMPR) {
        this.ENTIDAD_NOMBRE_EMPR = ENTIDAD_NOMBRE_EMPR;
    }
public String GetENTIDAD_CLAVE_EMPR() {
        return ENTIDAD_CLAVE_EMPR;
    }

    public void SetENTIDAD_CLAVE_EMPR(String ENTIDAD_CLAVE_EMPR) {
        this.ENTIDAD_CLAVE_EMPR = ENTIDAD_CLAVE_EMPR;
    }
public String GetMUNICIPIO_NOMBRE_EMPR() {
        return MUNICIPIO_NOMBRE_EMPR;
    }

    public void SetMUNICIPIO_NOMBRE_EMPR(String MUNICIPIO_NOMBRE_EMPR) {
        this.MUNICIPIO_NOMBRE_EMPR = MUNICIPIO_NOMBRE_EMPR;
    }
public String GetMUNICIPIO_CLAVE_EMPR() {
        return MUNICIPIO_CLAVE_EMPR;
    }

    public void SetMUNICIPIO_CLAVE_EMPR(String MUNICIPIO_CLAVE_EMPR) {
        this.MUNICIPIO_CLAVE_EMPR = MUNICIPIO_CLAVE_EMPR;
    }
public String GetLATITUD_EMPR1() {
        return LATITUD_EMPR1;
    }

    public void SetLATITUD_EMPR1(String LATITUD_EMPR1) {
        this.LATITUD_EMPR1 = LATITUD_EMPR1;
    }
public String GetLONGITUD_EMPR1() {
        return LONGITUD_EMPR1;
    }

    public void SetLONGITUD_EMPR1(String LONGITUD_EMPR1) {
        this.LONGITUD_EMPR1 = LONGITUD_EMPR1;
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
            GetID_ACTOR(),
            GetACTOR(),
            GetDEFENSA_ACT(),
            GetNOMBRE_SINDICATO(),
            GetREG_ASOC_SINDICAL(),
            GetTIPO_SINDICATO(),
            GetOTRO_ESP_SINDICATO(),
            GetORG_OBRERA(),
            GetNOMBRE_ORG_OBRERA(),
            GetOTRO_ESP_OBRERA(),
            GetCANT_TRABAJA_INV(),
            GetHOMBRES(),
            GetMUJERES(),
            GetNO_IDENTIFICADO(),
            GetTIPO(),
            GetRFC_PATRON(),
            GetRAZON_SOCIAL_EMPR(),
            GetCALLE(),
            GetN_EXT(),
            GetN_INT(),
            GetCOLONIA(),
            GetCP(),
            GetENTIDAD_NOMBRE_EMPR(),
            GetENTIDAD_CLAVE_EMPR(),
            GetMUNICIPIO_NOMBRE_EMPR(),
            GetMUNICIPIO_CLAVE_EMPR(),
            GetLATITUD_EMPR1(),
            GetLONGITUD_EMPR1(),
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

