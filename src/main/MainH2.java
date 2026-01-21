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

        Server web = Server.createWebServer("-web", "-webAllowOthers", "-webPort", "8082").start();
        System.out.println("WEB: " + web.getURL());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Apagando H2 Web...");
            web.stop();
        }));

        Thread.currentThread().join();
    }
}