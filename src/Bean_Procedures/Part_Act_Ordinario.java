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
public class Part_Act_Ordinario extends Bean implements SQLData {

    private String SQLTypeName;
    private String NOMBRE_ORGANO_JURIS;
    private String CLAVE_ORGANO;
    private String EXPEDIENTE_CLAVE;
    private String ID_ACTOR;
    private String ACTOR;
    private String DEFENSA_ACT;
    private String SEXO;
    private String EDAD;
    private String OCUPACION;
    private String NSS;
    private String CURP;
    private String RFC_TRABAJADOR;
    private String JORNADA;
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
    private String COMENTARIOS;
    private String PERIODO;


    
  
    
    public Part_Act_Ordinario() {
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

    public String GetSEXO() {
        return SEXO;
    }

    public void SetSEXO(String SEXO) {
        this.SEXO = SEXO;
    }

    public String GetEDAD() {
        return EDAD;
    }

    public void SetEDAD(String EDAD) {
        this.EDAD = EDAD;
    }

    public String GetOCUPACION() {
        return OCUPACION;
    }

    public void SetOCUPACION(String OCUPACION) {
        this.OCUPACION = OCUPACION;
    }

    public String GetNSS() {
        return NSS;
    }

    public void SetNSS(String NSS) {
        this.NSS = NSS;
    }

    public String GetCURP() {
        return CURP;
    }

    public void SetCURP(String CURP) {
        this.CURP = CURP;
    }

    public String GetRFC_TRABAJADOR() {
        return RFC_TRABAJADOR;
    }

    public void SetRFC_TRABAJADOR(String RFC_TRABAJADOR) {
        this.RFC_TRABAJADOR = RFC_TRABAJADOR;
    }

    public String GetJORNADA() {
        return JORNADA;
    }

    public void SetJORNADA(String JORNADA) {
        this.JORNADA = JORNADA;
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
            GetID_ACTOR(),
            GetACTOR(),
            GetDEFENSA_ACT(),
            GetSEXO(),
            GetEDAD(),
            GetOCUPACION(),
            GetNSS(),
            GetCURP(),
            GetRFC_TRABAJADOR(),
            GetJORNADA(),
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
