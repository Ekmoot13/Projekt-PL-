CREATE TABLE liga_Kluby (
    Skrot VARCHAR(10) PRIMARY KEY,
    Nazwa VARCHAR(100)
);

CREATE TABLE liga_Zawodnik (
    ID_Zawodnika INT PRIMARY KEY,
    Imie VARCHAR(50),
    Nazwisko VARCHAR(50),
    Email VARCHAR(100),
    Pozycja_na_lodce VARCHAR(50),
    Data_wstapienia_do_PLZ DATE,
    Numer_licencji VARCHAR(50),
    Numer_ubezpieczenia VARCHAR(50)
);

CREATE TABLE liga_Regaty (
    ID_Regat INT PRIMARY KEY,
    Nazwa VARCHAR(100),
    Liga_Poziom VARCHAR(50),
    Miasto VARCHAR(100),
    Numer_Rundy INT,
    Rok INT
);

CREATE TABLE liga_Wyscigi (
    ID_wyscigu INT PRIMARY KEY,
    ID_Regat INT,
    Numer_wyscigu VARCHAR(10),
    Finalowy BOOLEAN,
    FOREIGN KEY (ID_Regat) REFERENCES liga_Regaty(ID_Regat) ON DELETE CASCADE
);

CREATE TABLE liga_Wystepowanie_w_regatach (
    ID_wystepowania INT AUTO_INCREMENT PRIMARY KEY,
    ID_Zawodnika INT,
    ID_Regat INT,
    Skrot VARCHAR(10),
    Trening VARCHAR(20),
    FOREIGN KEY (ID_Zawodnika) REFERENCES liga_Zawodnik(ID_Zawodnika) ON DELETE CASCADE,
    FOREIGN KEY (ID_Regat) REFERENCES liga_Regaty(ID_Regat) ON DELETE CASCADE,
    FOREIGN KEY (Skrot) REFERENCES liga_Kluby(Skrot) ON DELETE CASCADE
);

CREATE TABLE liga_Miejsca (
    ID_miejsca INT PRIMARY KEY,
    ID_wyscigu INT,
    Skrot VARCHAR(10),
    Zajete_miejsce DECIMAL(5,2),
    Kary INT,
    Numer_lodki VARCHAR(50),
    FOREIGN KEY (ID_wyscigu) REFERENCES liga_Wyscigi(ID_wyscigu) ON DELETE CASCADE,
    FOREIGN KEY (Skrot) REFERENCES liga_Kluby(Skrot) ON DELETE CASCADE
);
