# -*- coding: utf-8 -*-
"""
export_wystepowania_full_from_wystepy.py
----------------------------------------
Wersja produkcyjna:
- wczytuje plik z występami zawodników (Zawodnik,ID_klubu,rok,regaty,numer rundy,poziom_ligi)
- wczytuje all_wynikRegat.csv (żeby pobrać WynikWRegatach dla QC)
- generuje pełny wsad do tabeli liga_Wystepowanie_w_regatach w CSV
    kolumny: ID_wystepowania,ID_Zawodnika,ID_Regat,Skrot,Trening
- generuje też plik QC z WynikWRegatach

Jak uruchomić:
Po prostu Run ▶ w PyCharmie.
"""

import pandas as pd
import numpy as np
import re
import os
import hashlib
import unicodedata

# ===== ŚCIEŻKI WE/WY =====
WYSTEPY_PATH = "./mnt/data/zawodnicy/zawodnicy_ekstraklasa_wystepy_full.csv"   # <- ten Twój nowy plik
WYNIKI_PATH  = "./mnt/data/output/all_wynikRegat.csv"

OUT_CSV_FULL = "wystepowanie_w_regatach_FULL.csv"         # do importu do bazy
OUT_QC_FULL  = "_QC_wystepowanie_with_wynik_FULL.csv"     # tylko kontrolnie


# ===== FUNKCJE ID =====

def strip_accents_lower(s: str) -> str:
    """
    Usuwa polskie znaki, robi lowercase i czyści wielokrotne spacje.
    Używane do budowania deterministycznego ID zawodnika.
    """
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()

def generate_player_id_only_name(fullname: str) -> str:
    """
    ID_Zawodnika zależne tylko od imienia+nazwiska (po normalizacji).
    Zawsze 8 cyfr.
    """
    norm = strip_accents_lower(fullname)
    base = f"zawodnik|name_norm={norm}"
    h = hashlib.sha1(base.encode()).digest()
    num = int.from_bytes(h[:4], "big") % 100_000_000
    return f"{num:08d}"

def generate_numeric_id(typ: str, liga_poziom: str, **params) -> str:
    """
    To samo co wcześniej do ID_Regat i ID_wystepowania.
    Kolejność parametrów nie ma znaczenia, hash jest deterministyczny.
    """
    param_string = f"liga_poziom={liga_poziom}|" + "|".join(
        f"{k}={v}" for k, v in sorted(params.items())
    )
    base_string = f"{typ}|{param_string}"
    h = hashlib.sha1(base_string.encode()).digest()
    num = int.from_bytes(h[:4], "big") % 100_000_000
    return f"{num:08d}"


# ===== FUNKCJE POMOCNICZE =====

def load_csv_any(path: str) -> pd.DataFrame:
    for enc in ["utf-8-sig", "utf-8", "cp1250", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return pd.read_csv(path)

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def liga_tag_for_row(poz_ligi: str) -> str:
    """
    Poziom_ligi -> którą etykietę w nawiasie bierzemy z kolumny 'regaty'
    Ekstraklasa -> "(EX)"
    1 Liga      -> "(1L)"
    """
    txt = str(poz_ligi).strip().lower()
    if "ekstra" in txt:
        return "EX"
    if "1" in txt:
        return "1L"
    # fallback
    return "EX"

def pick_regaty_for_liga(regaty_cell: str, poziom_ligi: str) -> str:
    """
    Mamy np:
      "Świnoujście (EX) / Szczecin (1L)"
    i poziom_ligi = "1 Liga" → chcemy "Szczecin"
    Ekstraklasa → chcemy "Świnoujście"

    Zasada:
    - rozbij po "/"
    - każdy fragment wygląda jak "<nazwa> (TAG)"
    - TAG porównujemy z oczekiwaną etykietą ligi ("EX" vs "1L").
    - zwracamy nazwę bez nawiasu.

    Jeśli nic nie pasuje, zwracamy pierwszy kawałek przed "(" jako fallback.
    """
    want = liga_tag_for_row(poziom_ligi)  # 'EX' albo '1L'
    parts = [p.strip() for p in str(regaty_cell).split("/")]

    found_name = None
    for p in parts:
        # p np. "Świnoujście (EX)" lub "Szczecin (1L)"
        m = re.match(r"(.+?)\s*\(([^)]+)\)\s*$", p)
        if m:
            name = m.group(1).strip()
            tag = m.group(2).strip()
            # dopasuj 'EX' do Ekstraklasy, '1L' do 1 Liga
            if tag.lower() == want.lower():
                found_name = name
                break

    if found_name is not None:
        return found_name

    # fallback: weź pierwszy fragment do nawiasu z pierwszej części
    m2 = re.match(r"(.+?)\s*\(", parts[0])
    if m2:
        return m2.group(1).strip()
    return parts[0].strip()


# ===== GŁÓWNA LOGIKA =====

def main():
    print("🏁 Generuję pełny wsad występowania z pliku występów zawodników...")

    # 1. Wczytaj oba pliki
    wyst = norm_cols(load_csv_any(WYSTEPY_PATH))
    wyniki = norm_cols(load_csv_any(WYNIKI_PATH))

    # 2. Sprawdź czy mamy wymagane kolumny
    # wystepy file
    required_wyst_cols = {
        "Zawodnik",
        "ID_klubu",
        "rok",
        "regaty",
        "numer rundy",
        "poziom_ligi"
    }
    if not required_wyst_cols.issubset(wyst.columns):
        raise SystemExit(
            f"❌ {WYSTEPY_PATH} nie ma wszystkich kolumn {required_wyst_cols}. "
            f"Mam: {list(wyst.columns)}"
        )

    # all_wynikRegat file
    required_wyn_cols = {"regaty", "klub", "miejsceWRegatach"}
    if not required_wyn_cols.issubset(wyniki.columns):
        raise SystemExit(
            f"❌ {WYNIKI_PATH} nie ma wszystkich kolumn {required_wyn_cols}. "
            f"Mam: {list(wyniki.columns)}"
        )

    # 3. Przygotuj lookup z wyników regat (miejsce klubu)
    wyniki["regaty"] = wyniki["regaty"].astype(str)
    wyniki["klub"] = wyniki["klub"].astype(str)

    miejsce_map = {
        (row.regaty, row.klub): row.miejsceWRegatach
        for row in wyniki[["regaty", "klub", "miejsceWRegatach"]].itertuples(index=False)
    }

    istniejące_id_regat = set(wyniki["regaty"].astype(str).unique())

    rows_db = []   # do importu do bazy
    rows_qc = []   # kontrolne z WynikWRegatach i nazwą regat

    # 4. Iteruj po każdym występie z pliku występów
    for _, r in wyst.iterrows():
        full_name = str(r["Zawodnik"]).strip()
        skrot_klubu = str(r["ID_klubu"]).strip()
        rok = int(r["rok"])
        poziom_ligi = str(r["poziom_ligi"]).strip()
        runda = int(r["numer rundy"])
        opis_regat = str(r["regaty"]).strip()

        # wybieramy właściwe regaty tekstowo tylko informacyjnie
        wybrane_regaty_nazwa = pick_regaty_for_liga(opis_regat, poziom_ligi)

        # liczymy ID_Zawodnika (tylko imię+nazwisko!)
        zawodnik_id = generate_player_id_only_name(full_name)

        # liczymy ID_Regat deterministycznie jak zawsze
        id_regat = generate_numeric_id(
            "regaty",
            poziom_ligi,
            rok=rok,
            runda=runda
        )

        # czy te regaty faktycznie istnieją w wynikach?
        # (czy mamy taki ID_Regat i wynik dla tego klubu)
        if id_regat not in istniejące_id_regat:
            # brak takich regat w all_wynikRegat -> pomijamy
            # (albo to znaczy że nie mamy jeszcze wyników w tamtym csv)
            continue

        if (id_regat, skrot_klubu) not in miejsce_map:
            # klub nie ma wpisanego wyniku dla tych regat -> też pomijamy
            continue

        wynik_klubu = miejsce_map[(id_regat, skrot_klubu)]

        # wyliczamy ID_wystepowania
        id_wystepowania = generate_numeric_id(
            "wystepowanie",
            poziom_ligi,
            rok=rok,
            runda=runda,
            zawodnik=zawodnik_id,
            klub=skrot_klubu
        )

        # rekord finalny do bazy
        rows_db.append({
            "ID_wystepowania": int(id_wystepowania),
            "ID_Zawodnika": int(zawodnik_id),
            "ID_Regat": int(id_regat),
            "Skrot": skrot_klubu,
            "Trening": ""  # na razie puste
        })

        # rekord QC, żebyśmy widzieli kontrolnie co z czego
        rows_qc.append({
            "ID_wystepowania": int(id_wystepowania),
            "ID_Zawodnika": int(zawodnik_id),
            "ID_Regat": int(id_regat),
            "Skrot": skrot_klubu,
            "WynikWRegatach": wynik_klubu,
            "Poziom_Ligi": poziom_ligi,
            "Rok": rok,
            "Runda": runda,
            "Regaty_wybrane": wybrane_regaty_nazwa,
            "Zawodnik": full_name
        })

    # 5. sklej DataFrame, usuń duplikaty bezpieczeństwa
    df_db = pd.DataFrame(rows_db, columns=[
        "ID_wystepowania",
        "ID_Zawodnika",
        "ID_Regat",
        "Skrot",
        "Trening"
    ]).drop_duplicates()

    df_qc = pd.DataFrame(rows_qc, columns=[
        "ID_wystepowania",
        "ID_Zawodnika",
        "ID_Regat",
        "Skrot",
        "WynikWRegatach",
        "Poziom_Ligi",
        "Rok",
        "Runda",
        "Regaty_wybrane",
        "Zawodnik"
    ]).drop_duplicates()

    # 6. zapisz CSV
    df_db.to_csv(OUT_CSV_FULL, index=False, encoding="utf-8-sig")
    df_qc.to_csv(OUT_QC_FULL, index=False, encoding="utf-8-sig")

    # 7. podsumowanie w terminalu
    print("✅ GOTOWE")
    print(f"→ {OUT_CSV_FULL}  ({len(df_db)} rekordów)  <-- IMPORTUJ DO BAZY liga_Wystepowanie_w_regatach")
    print(f"→ {OUT_QC_FULL}   ({len(df_qc)} rekordów)  <-- kontrolnie (z WynikWRegatach, nazwą regat, itp.)")

    print("\nPodgląd pierwszych 15 rekordów do importu:")
    print(df_db.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
