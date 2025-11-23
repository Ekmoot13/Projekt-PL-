-- INIT SQL (updated pod Zestawienie Klubów + Wariant Klubu, ID_wariantu_klubu = INT)
-- MySQL 8+

-- =========================
-- T A B E L E   B A Z O W E
-- =========================

-- Stała tożsamość klubu (np. "AZS Politechnika Gdańska")
CREATE TABLE IF NOT EXISTS liga_ZestawienieKlubow (
    ID_zestawienia_klubow INT PRIMARY KEY,
    Nazwa VARCHAR(100) NOT NULL
);

-- Wariant nazwy/skrótów klubu w konkretnych latach itp.
-- ID_wariantu_klubu = deterministyczne ID z Pythona (INT)
CREATE TABLE IF NOT EXISTS liga_KlubWariant (
    ID_wariantu_klubu INT PRIMARY KEY,
    Skrot VARCHAR(30) NOT NULL,
    Nazwa VARCHAR(100) NOT NULL,
    ID_zestawienia_klubow INT NOT NULL,
    CONSTRAINT fk_kw_zestawienie
        FOREIGN KEY (ID_zestawienia_klubow)
        REFERENCES liga_ZestawienieKlubow(ID_zestawienia_klubow)
        ON DELETE CASCADE
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
    ID_Regat INT NOT NULL,
    Numer_wyscigu VARCHAR(10),
    Finalowy BOOLEAN,
    FOREIGN KEY (ID_Regat) REFERENCES liga_Regaty(ID_Regat) ON DELETE CASCADE
);

-- Wystąpienie zawodnika w danych regatach, powiązane z wariantem klubu
CREATE TABLE IF NOT EXISTS liga_Wystepowanie_w_regatach (
    ID_wystepowania INT AUTO_INCREMENT PRIMARY KEY,
    ID_Zawodnika INT NOT NULL,
    ID_Regat INT NOT NULL,
    ID_wariantu_klubu INT NOT NULL,
    WynikWRegatach INT,
    Trening VARCHAR(20),
    FOREIGN KEY (ID_Zawodnika) REFERENCES liga_Zawodnik(ID_Zawodnika) ON DELETE CASCADE,
    FOREIGN KEY (ID_Regat) REFERENCES liga_Regaty(ID_Regat) ON DELETE CASCADE,
    FOREIGN KEY (ID_wariantu_klubu) REFERENCES liga_KlubWariant(ID_wariantu_klubu) ON DELETE CASCADE
);

-- Wynik w pojedynczym wyścigu, powiązany z wariantem klubu
CREATE TABLE IF NOT EXISTS liga_Miejsca (
    ID_miejsca INT AUTO_INCREMENT PRIMARY KEY,
    ID_wyscigu INT NOT NULL,
    ID_wariantu_klubu INT NOT NULL,
    Zajete_miejsce DECIMAL(5,2),
    Kary INT,
    Numer_lodki VARCHAR(50),
    FOREIGN KEY (ID_wyscigu) REFERENCES liga_Wyscigi(ID_wyscigu) ON DELETE CASCADE,
    FOREIGN KEY (ID_wariantu_klubu) REFERENCES liga_KlubWariant(ID_wariantu_klubu) ON DELETE CASCADE
);

-- Ręcznie wprowadzony wynik regat dla klubu (wariantu klubu)
DROP TABLE IF EXISTS liga_WynikRegatManual;

CREATE TABLE liga_WynikRegatManual (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    regaty INT NOT NULL,
    ID_wariantu_klubu INT NOT NULL,
    miejsceWRegatach INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_wrm_regaty
        FOREIGN KEY (regaty) REFERENCES liga_Regaty(ID_Regat) ON DELETE CASCADE,
    CONSTRAINT fk_wrm_wariant
        FOREIGN KEY (ID_wariantu_klubu) REFERENCES liga_KlubWariant(ID_wariantu_klubu) ON DELETE CASCADE,
    CONSTRAINT uq_wrm_regaty_klub UNIQUE (regaty, ID_wariantu_klubu)
);

-- =========================
-- I N D E K S Y  pomocnicze
-- =========================

CREATE INDEX IF NOT EXISTS idx_kw_skrot
    ON liga_KlubWariant (Skrot);

CREATE INDEX IF NOT EXISTS idx_wyscigi_id_regat
    ON liga_Wyscigi(ID_Regat);

CREATE INDEX IF NOT EXISTS idx_miejsca_id_wyscigu
    ON liga_Miejsca(ID_wyscigu);

CREATE INDEX IF NOT EXISTS idx_miejsca_id_wariantu
    ON liga_Miejsca(ID_wariantu_klubu);

CREATE INDEX IF NOT EXISTS idx_wyst_zawodnik
    ON liga_Wystepowanie_w_regatach(ID_Zawodnika);

CREATE INDEX IF NOT EXISTS idx_wyst_regaty
    ON liga_Wystepowanie_w_regatach(ID_Regat);

CREATE INDEX IF NOT EXISTS idx_wyst_id_wariantu
    ON liga_Wystepowanie_w_regatach(ID_wariantu_klubu);

CREATE INDEX idx_wrm_regaty_miejsce
    ON liga_WynikRegatManual(regaty, miejsceWRegatach);

CREATE INDEX idx_wrm_id_wariantu
    ON liga_WynikRegatManual(ID_wariantu_klubu);
