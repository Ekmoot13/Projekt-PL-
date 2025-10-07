-- INIT SQL (updated) — struktura bez redundancji + widok z wynikami regat
-- Silnik: MySQL 8+ (window functions dla DENSE_RANK)
-- Kolejność: tabele bazowe -> indeksy -> widok

-- =========================
-- TABEL E B A Z O W E
-- =========================

CREATE TABLE IF NOT EXISTS liga_Kluby (
    Skrot VARCHAR(30) PRIMARY KEY,
    Nazwa VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS liga_Zawodnik (
    ID_Zawodnika INT PRIMARY KEY,
    Imie VARCHAR(50),
    Nazwisko VARCHAR(50),
    Email VARCHAR(100),
    Pozycja_na_lodce VARCHAR(50),
    Data_wstapienia_do_PLZ DATE,
    Numer_licencji VARCHAR(50),
    Numer_ubezpieczenia VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS liga_Regaty (
    ID_Regat INT PRIMARY KEY,
    Nazwa VARCHAR(100),
    Liga_Poziom VARCHAR(50),
    Miasto VARCHAR(100),
    Numer_Rundy INT,
    Rok INT
);

CREATE TABLE IF NOT EXISTS liga_Wyscigi (
    ID_wyscigu INT PRIMARY KEY,
    ID_Regat INT,
    Numer_wyscigu VARCHAR(10),
    Finalowy BOOLEAN,
    FOREIGN KEY (ID_Regat) REFERENCES liga_Regaty(ID_Regat) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS liga_Wystepowanie_w_regatach (
    ID_wystepowania INT AUTO_INCREMENT PRIMARY KEY,
    ID_Zawodnika INT,
    ID_Regat INT,
    Skrot VARCHAR(10),
    WynikWRegatach INT,
    Trening VARCHAR(20),
    FOREIGN KEY (ID_Zawodnika) REFERENCES liga_Zawodnik(ID_Zawodnika) ON DELETE CASCADE,
    FOREIGN KEY (ID_Regat) REFERENCES liga_Regaty(ID_Regat) ON DELETE CASCADE,
    FOREIGN KEY (Skrot) REFERENCES liga_Kluby(Skrot) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS liga_Miejsca (
    ID_miejsca INT PRIMARY KEY,
    ID_wyscigu INT,
    Skrot VARCHAR(10),
    Zajete_miejsce DECIMAL(5,2),
    Kary INT,
    Numer_lodki VARCHAR(50),
    FOREIGN KEY (ID_wyscigu) REFERENCES liga_Wyscigi(ID_wyscigu) ON DELETE CASCADE,
    FOREIGN KEY (Skrot) REFERENCES liga_Kluby(Skrot) ON DELETE CASCADE
);


DROP TABLE IF EXISTS liga_WynikRegatManual;
CREATE TABLE liga_WynikRegatManual (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    regaty INT NOT NULL,
    klub VARCHAR(10) NOT NULL,
    miejsceWRegatach INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_wrm_regaty FOREIGN KEY (regaty) REFERENCES liga_Regaty(ID_Regat) ON DELETE CASCADE,
    CONSTRAINT fk_wrm_klub   FOREIGN KEY (klub)   REFERENCES liga_Kluby(Skrot)   ON DELETE CASCADE,
    CONSTRAINT uq_wrm_regaty_klub UNIQUE (regaty, klub),
    CONSTRAINT ck_wrm_miejsce CHECK (miejsceWRegatach >= 1)
) ENGINE=InnoDB;

-- =========================
-- I N D E K S Y  pomocnicze
-- =========================

CREATE INDEX IF NOT EXISTS idx_wyscigi_id_regat    ON liga_Wyscigi(ID_Regat);
CREATE INDEX IF NOT EXISTS idx_miejsca_id_wyscigu  ON liga_Miejsca(ID_wyscigu);
CREATE INDEX IF NOT EXISTS idx_miejsca_skrot       ON liga_Miejsca(Skrot);
CREATE INDEX IF NOT EXISTS idx_wyst_zawodnik       ON liga_Wystepowanie_w_regatach(ID_Zawodnika);
CREATE INDEX IF NOT EXISTS idx_wyst_regaty         ON liga_Wystepowanie_w_regatach(ID_Regat);
CREATE INDEX IF NOT EXISTS idx_wyst_skrot          ON liga_Wystepowanie_w_regatach(Skrot);

CREATE INDEX idx_wrm_regaty_miejsce ON liga_WynikRegatManual(regaty, miejsceWRegatach);
CREATE INDEX idx_wrm_klub          ON liga_WynikRegatManual(klub);
