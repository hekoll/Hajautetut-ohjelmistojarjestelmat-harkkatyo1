// HAJAUTETUT OHJELMISTOJÄRJESTELMÄT JA PILVIPALVELUT HARJOITUSTYÖ
// Tekijät: Satu Jantunen, Pinja Palm ja Helena Ollila

// Tarvittavat paketit

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;

public class XnSummausPalvelu implements Runnable {

  //Luokan muuttujat
  
  private static ArrayList<Integer> lokerot = new ArrayList<Integer>();
  private static ArrayList<Integer> summat = new ArrayList<Integer>();
  private static ArrayList<Integer> portit = new ArrayList<Integer>();
  private static ArrayList<SummausPalvelu> summausPalvelimet = new ArrayList<SummausPalvelu>();
  private static boolean yhteysKaynnissa;
  private int t;
  
  public static void main(String[] args){
  // Luo summauspalvelun ja ajaa sen run metodin.
    new Thread(new XnSummausPalvelu()).start();
  } // main metodi loppuu

  // Koska rajapinnassa Runnable on jo run metodi, korvataan se uudella run 
  // metodilla.
  
  @Override 
  public void run (){

    try {
        
      Socket sokettiY = muodostaTCPyhteys();

      InputStream iS = sokettiY.getInputStream(); // Luodaan olio/tietovirta
      OutputStream oS = sokettiY.getOutputStream();
      ObjectOutputStream oOut = new ObjectOutputStream(oS);
      ObjectInputStream oIn = new ObjectInputStream(iS);
      
      try {
        sokettiY.setSoTimeout(5000); // odotetaan 5 sekuntia
        t = (int) oIn.readInt(); // palvelin Y ilmoittaa kokonaisluvun t
        System.out.println("Pitaa luoda " + t + " porttia");
        portit = luoPortit(t);
      } // try lohko loppuu
      
      catch(SocketException e) {
        System.out.println("Ajan ylitys");
        oOut.writeInt(-1); // X välittää luvun -1 Y:lle, jos Y ei
        oOut.flush();      // lähetä lukua t 5 sekunnin sisällä. 
        yhteysKaynnissa = false;
        System.exit(0); // X lopettaa itsensä hallitusti
       } // catch lohko loppuu
      
      summausPalvelimet = alustaSummauspalvelut(t, summat, portit);
      lahetaPortit(portit, oOut);
      yhteysKaynnissa= true;

      while (yhteysKaynnissa){
        YKysyy(oIn, oOut, sokettiY, summausPalvelimet);
      } // while silmukka loppuu

      sokettiY.close();
    } // try lohko loppuu
    
    catch (Exception e) {
      e.printStackTrace();
    } // catch lohko loppuu
    
  } // run metodi loppuu
  
  // Metodissa lahetaUDPpaketti luodaan UDP paketti, jossa TCP-portin numero 
  // (1994) lähetetään palvelimen Y porttiin 3126.
  
  private static void lahetaUDPpaketti () throws IOException {
    
    InetAddress osoiteY = InetAddress.getLocalHost();
    String porttinumero = "1994";
    DatagramSocket sokettiUDP = new DatagramSocket();
    byte[] dataPorttiX = porttinumero.getBytes();
    DatagramPacket UDPpaketti = new DatagramPacket (dataPorttiX, dataPorttiX.length, osoiteY, 3126);
    
    sokettiUDP.send(UDPpaketti);
    sokettiUDP.close();
    System.out.println("UDP paketti lahetetty");
  } // lahetaUDPpaketti metodi loppuu

  // Metodi muodostaTCPyhteys kuuntelee sokettia max 5 sekunttia ja yrittää
  // lähettää UDP pakettia viisi kertaa.
  // Jos paketin lähetys ei onnistu viidennelläkään yrityksellä, X palvelin 
  // lopettaa itsensä.

  private static Socket muodostaTCPyhteys () throws IOException {
      
    int yritys = 1;
    ServerSocket vastaanottajaSoketti = new ServerSocket(1994); 
    Socket YnTCPsoketti = null;
    
    //Kokeillaan viisi kertaa muodostaa yhteys
    while(yritys<=5){ 
       try{
        lahetaUDPpaketti(); // Yritetään lähettää UDP pakettia Y:lle.
        vastaanottajaSoketti.setSoTimeout(5000); // odottaa 5 sekuntia
        YnTCPsoketti = vastaanottajaSoketti.accept();
        vastaanottajaSoketti.close();
        System.out.println("Yhteys muodostettu");
        break; // lopetetaan yritykset, kun yhteys saadaan muodostettua 
      } // try lohko loppuu
       
      catch(SocketException e){
        yritys = yritys + 1;  
        System.out.println("Yhteydenotto ei onnistunut");    
        if(yritys==6){ // Jos yhteydenotto ei onnistu viidennellä yrityksellä 
          System.exit(0);  // lopetetaan X
        } // if ehto loppuu 
      } // catch loppuu
    } // while silmukka loppuu
    
    return YnTCPsoketti;
  } // metodi muodostaTCPyhteys loppuu

// Metodi luoPortteja luo t määrän portti numeroita.
// Portti numerot tallennetaan listaan, joka palautetaan.
  
private static ArrayList<Integer> luoPortit(int t){
    
    int p = 1995;
    ArrayList<Integer> porttiLista = new ArrayList<Integer>();
    for(int i = 0; i<t; i++){
      porttiLista.add(p);
      p = p + 1;
      System.out.println(porttiLista.get(i));
    } //for silmukka loppu
    
    return porttiLista;
  } // metodi luoPortit loppuu

  // Metodi lahetaPortit saa listan porttinumeroita ja lähettää ne palvelimelle Y.

  private static void lahetaPortit(ArrayList<Integer> p, ObjectOutputStream oOut) throws IOException {
      
    for(int i = 0; i<p.size(); i++){
      int a = p.get(i); 
      oOut.writeInt(a);
      oOut.flush();
    } // for silmukka loppuu
  } // metodi lahetaPortit loppuu

  // Metodi alustaSummauspalvelut luo t kappaletta summauspalvelijoita luotuihin
  // portteihin ja käynnistää ne.

  private static ArrayList<SummausPalvelu> alustaSummauspalvelut(int t, ArrayList<Integer> summat, ArrayList<Integer> portit){

    for(int i=0; i<t; i++){  
        SummausPalvelu a = new SummausPalvelu(i, portit.get(i));
        System.out.println(portit.get(i));
        summausPalvelimet.add(a);
        summat.add(i, 0);  
        System.out.println("Saie " + i + " kaynnistetty");
      } //for silmukka loppuu
    
    for(int i=0; i<t; i++){
      new Thread(summausPalvelimet.get(i)).start();
    }// for silmukka loppuu
    
    return summausPalvelimet;
  } // metodi alustaSummausPalvelut loppuu

  // Y kysyy summauspalvelijoihin liittyviä tietoja X:ltä.

  private synchronized void YKysyy(ObjectInputStream oIn, ObjectOutputStream oOut, Socket soketti, ArrayList<SummausPalvelu> summausPalvelimet){ 
      
    try{
	Thread.sleep(1000);
    } // try lohko loppuu
    catch(Exception e){
	System.out.println("Ei annettu nukkua tarpeeksi!");
    } // catch lohko loppuu
    
    try{
      soketti.setSoTimeout(60000); // odottaa 1 min
      // Y lähettää kokonaisluvun, joka tarkoittaa kysymystä.
      int kysymys = oIn.readInt(); 
      
      System.out.println("Y lahettaa luvun " + kysymys);
      
      // Kun Y lähettää numeron 0, X sulkee TCP yhteyden palvelimeen Y, lopettaa 
      // kesken olevat summauspalvelijat ja lopettaa X suorituksen.
      if(kysymys==0){
        System.out.println("Y lopettaa X:n tarjoaman palvelun kayton");
        soketti.close(); // X sulkee TCP-yhteyden palvelimeen Y
	yhteysKaynnissa=false;
        System.exit(0); // X lopettaa itsensä
      } // if lohko loppuu

      // Kun Y lähettää numeron 1, X:n pitää lähettää tähän mennessä
      // välitettyjen lukujen kokonaissumma.
      if(kysymys==1){
        System.out.println("Y kysyy valitettyjen lukujen kokonaissummaa.");
        synchronized(summat){
            int kokonaisSumma = 0;
            for(int i=0; i<summat.size(); i++){
                kokonaisSumma = kokonaisSumma + summat.get(i);
            } // for silmukka loppuu 
            System.out.println("Kokonaissumma on " + kokonaisSumma);
            oOut.writeInt(kokonaisSumma);
            oOut.flush();
        } // lukitus loppuu
      } // if lohko loppuu
    
      // Kun Y lähettää numeron 2, X:n pitää lähettää mille summauspalvelijalle
      // välitettyjen lukujen summa on suurin.
      if(kysymys==2){
       System.out.println("Y kysyy suurimman summan omaavan palvelijan id");
        synchronized(summat){
            int max = Collections.max(summat);
            int id = summat.indexOf(max)+1;
            System.out.println("Suurimman summan omaavan palvelijan id on " + id);
            oOut.writeInt(id);
            oOut.flush();
        } // lukitus loppuu
      } // if lohko loppuu
    
    // Kun Y lähettää numeron 3, X:n pitää lähettää mikä on tähän mennessä
    // kaikille summauspalvelimille välitettyjen lukujen kokonaissumma. 
      if(kysymys==3){
       System.out.println("Y kysyy lukujen kokonaismaaraa.");
       synchronized(lokerot){
            int kokonaisMaara = lokerot.size();
            System.out.println("Kokonaismaara on " + kokonaisMaara);
            oOut.writeInt(kokonaisMaara);
            oOut.flush();
       } // lukitus loppuu
      } // if lohko loppuu

     // Jos Y lähettää jonkin muun numeron, X:n pitää lähettää numero -1.
      if(kysymys!=0 && kysymys!=1 && kysymys!=2 && kysymys!=3){
       System.out.println("Y:n lahettama kokonaisluku ei vastaa kysymyksia, X lahettaa luvun -1");
       oOut.writeInt(-1);
       oOut.flush();
      } // if lohko loppuu
      
    } // try lohko loppuu
    
    catch(Exception e){
      System.out.println("Y ei lahettanyt kyselya minuutin kuluessa");
      System.exit(0);
    } // catch lohko loppuu
  } //metodi YKysyy loppuu

// Luokka SummausPalvelija, joka laskee lukujen summat.

 static class SummausPalvelu implements Runnable{

  // Luokan muuttujat
     
  private final int portti;
  private final int saikeenId;
  private int summa=0;
  private ServerSocket vastaanottajaSoketti;

  // konstruktori
  SummausPalvelu(int saikeenId, int portti){
    this.portti=portti;
    this.saikeenId=saikeenId;
    } // konstruktori loppuu

  @Override
  public void run(){
      
    try{
      vastaanottajaSoketti = new ServerSocket(portti);
      // Muodostetaan yhteys Y ja säikeiden välille
      Socket soketti = vastaanottajaSoketti.accept(); 

      // Avataan oliovirrat sisääntulevalle liikenteelle
      InputStream iS = soketti.getInputStream(); 
      ObjectInputStream oIn = new ObjectInputStream(iS);

    while(yhteysKaynnissa){
      int luku = oIn.readInt();
      if(luku==0){
        break;
      } // if lohko loppuu
        
      summa = summat.get(saikeenId);
      summa = summa + luku;
      summat.set(saikeenId, summa);
      lokerot.add(luku); // tallennetaan tieto X palvelimelle
            
      System.out.println("Saikeen id on " + saikeenId + " Vastaanotti luvun " + luku);

    } // while silmukka loppuu
    soketti.close();
    } // try lohko loppuu
    
    catch(IOException e){
      System.out.println("Yhteys ei muodostettu");
    } // catch lohko loppuu
    
  } // metodi run loppuu
 } // luokka SummausPalvelu loppuu
 
}// luokka XnSummausPalvelu loppuu 