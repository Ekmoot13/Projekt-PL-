# -*- coding: utf-8 -*-
"""
tworzenie_wystepowania_z_listy_zawodnikow.py

Wejście:
  - mnt/data/Zawodnicy_1_Liga_2024.csv
      kolumny:
        Zawodnik  (np. "Max Pawłowski")
        Klub      (skrót, np. "YCG")
        Runda     (1, 2, 3, ...)

  - mnt/data/zawodnicy/zawodnicy.csv
      kolumny:
        ID_Zawodnika, Imie, Nazwisko

Wyjście:
  - mnt/data/output/wystepowanie_z_listy/wystepowanie_1_liga_2024.csv
      kolumny:
        ID_wystepowania, ID_Zawodnika, ID_Regat, Skrot, Trening
"""

import pandas as pd
from pathlib import Path
import unicodedata
import re
import hashlib

# ---- KONFIGURACJA DLA TEGO PLIKU ----
SRC_FILE = Path("mnt/data/występowanie/Zawodnicy_1_Liga_2024.csv")
ZAWODNICY_FILE = Path("mnt/data/zawodnicy/zawodnicy.csv")
OUT_DIR = Path("mnt/data/output/wystepowanie/wystepowanie_z_listy")

LIGA_POZIOM = "1 Liga"
ROK = 2024


# ---------- FUNKCJE POMOCNICZE ----------

def strip_accents_lower(s: str) -> str:
    """Usuwa polskie znaki, ścina spacje, zamienia na lower-case."""
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()


def generate_numeric_id(typ: str, liga_poziom: str, **params) -> str:
    """
    KANONICZNE ID jak w main.py:
      sha1(f"{typ}|liga_poziom={liga}|k1=v1|k2=v2|...") -> 8 cyfr

    Dla regat:
      generate_numeric_id("regaty", liga_poziom, rok=ROK, runda=RUNDA)
    """
    param_string = f"liga_poziom={liga_poziom}|" + "|".join(
        f"{k}={v}" for k, v in sorted(params.items())
    )
    base_string = f"{typ}|{param_string}"
    h = hashlib.sha1(base_string.encode()).digest()
    num = int.from_bytes(h[:4], "big") % 100_000_000
    return f"{num:08d}"


def normalize_liga_for_filename(liga: str) -> str:
    s = str(liga or "").strip()
    s_norm = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s_norm = s_norm.lower()
    s_norm = re.sub(r"[–\-]+", " ", s_norm)
    s_norm = re.sub(r"[^a-z0-9]+", " ", s_norm)
    s_norm = re.sub(r"\s+", "_", s_norm).strip("_")
    return s_norm


# ---------- GŁÓWNA LOGIKA ----------

def main():
    print("🏁 Start: tworzenie wystąpień z pliku Zawodnicy_1_Liga_2024.csv")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Wczytaj zawodników (kanoniczna lista)
    zaw = pd.read_csv(ZAWODNICY_FILE)
    zaw["full_norm"] = (zaw["Imie"].astype(str) + " " + zaw["Nazwisko"].astype(str)).map(strip_accents_lower)
    name_to_id = dict(zip(zaw["full_norm"], zaw["ID_Zawodnika"]))

    # 2. Wczytaj plik z listą zawodników
    df = pd.read_csv(SRC_FILE)
    df["full_norm"] = df["Zawodnik"].map(strip_accents_lower)

    # 3. Dopasuj ID_Zawodnika
    df["ID_Zawodnika_new"] = df["full_norm"].map(name_to_id)

    # 4. Brakujący zawodnicy
    missing = df[df["ID_Zawodnika_new"].isna()].copy()
    liga_norm = normalize_liga_for_filename(LIGA_POZIOM)

    if not missing.empty:
        missing_out = OUT_DIR / f"wystepowanie_lista_brakujacy_zawodnicy_{liga_norm}_{ROK}.csv"
        missing.to_csv(missing_out, index=False, encoding="utf-8-sig")
        print(f"⚠ Brak dopasowania dla {len(missing)} wierszy – zapisano do: {missing_out}")
        print("   Unikalne brakujące nazwiska:", sorted(missing['Zawodnik'].unique()))
    else:
        print("✅ Wszystkie wiersze mają dopasowanego zawodnika.")

    # 5. Tylko poprawne rekordy
    valid = df[~df["ID_Zawodnika_new"].isna()].copy()
    if valid.empty:
        print("⚠ Brak poprawnych wierszy do zapisania.")
        return

    valid["Runda"] = pd.to_numeric(valid["Runda"], errors="coerce").astype("Int64")

    rows = []
    for _, r in valid.iterrows():
        id_zaw = int(r["ID_Zawodnika_new"])
        skrot_klubu = str(r["Klub"]).strip()
        runda = int(r["Runda"])

        # KANONICZNE ID_REGAT: tylko liga + rok + runda
        id_regat = int(generate_numeric_id(
            "regaty", LIGA_POZIOM,
            rok=ROK, runda=runda
        ))

        # ID_wystepowania – też deterministyczne
        id_wyst = int(generate_numeric_id(
            "wystepowanie", LIGA_POZIOM,
            rok=ROK, runda=runda,
            regaty=id_regat,
            zawodnik=id_zaw,
            klub=skrot_klubu
        ))

        rows.append({
            "ID_wystepowania": "",
            "ID_Zawodnika": id_zaw,
            "ID_Regat": id_regat,
            "Skrot": skrot_klubu,
            "WynikWRegatach": "",
            "Trening": ""
        })

    out_df = pd.DataFrame(rows)
    out_path = OUT_DIR / f"wystepowanie_{liga_norm}_{ROK}.csv"
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"✅ Zapisano: {out_path} ({len(out_df)} rekordów)")
    print("🔎 Podgląd:")
    print(out_df.head(15).to_string(index=False))
    print("🎉 Gotowe.")


if __name__ == "__main__":
    main()
