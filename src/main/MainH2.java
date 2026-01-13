/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package main;

/**
 *
 * @author ANTONIO.CORIA
 */
import org.h2.tools.Server;

public class MainH2 {
  public static void main(String[] args) throws Exception {
   // Server tcp = Server.createTcpServer("-tcp", "-tcpAllowOthers", "-tcpPort", "9092").start();
    Server web = Server.createWebServer("-web", "-webAllowOthers", "-webPort", "8082").start();

   // System.out.println("TCP: " + tcp.getURL());
    System.out.println("WEB: " + web.getURL()); // abre esta  
    try {
    Class.forName("triggers.V3TrAudienciasJL");
    System.out.println("OK: triggers.V3TrAudienciasJL SI está en el classpath");
} catch (ClassNotFoundException e) {
    System.out.println("ERROR: triggers.V3TrAudienciasJL NO está en el classpath");
    e.printStackTrace();
}
    Thread.currentThread().join();
  }
}