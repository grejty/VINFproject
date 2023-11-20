**====== Téma: ======**

Zachytávanie údajov o profesionálnych hráčoch z hry Counter-Strike 

**====== Stránka: ======**

liquipedia.com 

**====== Pseudokód: ======**
1. Skontrolovanie robots.txt súboru a kontrola, či môzeme adresu crawlovať

2. Nastavenie time-outu na 30s (aby sme zabránili prípadnému IP banu)

3. Získanie URL adries na jednotlivé regióny hráčov za pomoci regexu

4. Crawlovanie cez URL regiónov a zachytávanie URL jednotlivých hráčov

5. Sťahovanie HTML kódov stránok s profilmi hráčov

6. Parsovanie a ukladanie dát do .csv súboru

7. Vytvorenie indexera, ktorý pre každú hodnotu zapíše jej polohu v súbore

8. Search funkcia, ktorá pomocou indexera odpovie na zadaný dopyt:

      **Input Query:**

        sycrone dukiiii


      **Output:**

        Nick: dukiiii, Years Active(Player): 2013 – Present

        Nick: sycrone, Years Active(Player): 2015 – 2021
   
      **Result:**
   
       The two players could have played together.

   ------------------------------------------------------------

      **Input Query:**

        Jee karl


      **Output:**

        Nick: Jee, Years Active(Player): 2021 – Present

        Nick: karl, Years Active(Player): 2009 – 2016, 2016 – 2017, 2020
   
      **Result:**
   
       The two players could not have played together.

**====== Útržok vyparsovaných dát: ======**

**Header:** Dáta

**Nick:** stikle-

**Overview:** Klesti "stikle-" Kola (born July 5, 1998) is an Albanian professional Counter-Strike: Global Offensive coach.

**Name:** Klesti Kola

**Nationality:** Albania

**Born:** July  5, 1998 (age 25)

**Status:** Active

**Status Years Active (Player):** 

**Status Years Active (Coach):** 2020 – Present

**Role:** Coach	

**Team:** Sangal Esports

**Approx. Total Winnings:**	$1,006

**====== Konzultácia č. 2 ======**

19.10.2023 - Crawluje dáta, používa regex, parsuje údaje. Do budúcej konzultácie urobí indexáciu.

**====== Konzultácia č. 3 ======**

3.11.2023 - Funkčná indexácia a search funkcia. Do budúcej konzultácie doplní dáta s wikipédiou. 

**====== Konzultácia č. 4 - Prezentácia ======**

16.11.2023 - Prezentácia OK. Do budúcej konzultácie paralelné spracovanie údajov.
