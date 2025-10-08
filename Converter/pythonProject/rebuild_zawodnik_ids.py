# -*- coding: utf-8 -*-
"""
rebuild_zawodnik_ids_by_name.py
--------------------------------
Przelicza deterministycznie ID_Zawodnika WYŁĄCZNIE z imienia i nazwiska
(bez klubu/ligi/roku) dla CAŁEGO pliku wejściowego.

Wejście (domyślnie):
  - zawodnicy_unique_all.csv

Wyjście:
  - zawodnicy_unique_all_with_ids.csv   (z nowym ID_Zawodnika)
  - id_map_old_to_new.csv               (mapowanie stary → nowy + diagnostyka)

Zasada ID:
  new_id = sha1("zawodnik|name_norm=<imię_nazwisko_po_normalizacji>")[:4] → 8 cyfr (mod 1e8)
  gdzie name_norm = lowercase, bez polskich znaków, pojedyncze spacje.
"""

import hashlib
import pandas as pd
import numpy as np
import re
import unicodedata
from typing import Tuple

# ==== ŚCIEŻKI (zmień jeśli potrzebujesz) ====
INPUT_PATH  = "zawodnicy_unique_all.csv"
OUT_ROSTER  = "zawodnicy_unique_all_with_ids.csv"
OUT_MAPPING = "id_map_old_to_new.csv"

# ==== FUNKCJE POMOCNICZE ====
def strip_accents_lower(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()

def generate_player_id(fullname: str) -> str:
    norm = strip_accents_lower(fullname)
    base = f"zawodnik|name_norm={norm}"
    h = hashlib.sha1(base.encode()).digest()
    num = int.from_bytes(h[:4], "big") % 100_000_000
    return f"{num:08d}"

def load_csv_any(path: str) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp1250", "latin1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return pd.read_csv(path)

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def pick_name_columns(df: pd.DataFrame) -> Tuple[str, str, str]:
    """
    Zwraca (tryb, colA, colB)
      tryb = "split"  -> colA=Imię, colB=Nazwisko
      tryb = "single" -> colA=pełne imię i nazwisko, colB=None
    """
    candidates = [
        ("split",  "Imię",             "Nazwisko"),
        ("split",  "Imie",             "Nazwisko"),
        ("split",  "FirstName",        "LastName"),
        ("single", "Imię i nazwisko",  None),
        ("single", "Zawodnik",         None),
    ]
    for mode, a, b in candidates:
        if mode == "split" and a in df.columns and b in df.columns:
            return mode, a, b
        if mode == "single" and a in df.columns:
            return mode, a, None

    # heurystyka ostatniej szansy
    imie_guess = next((c for c in df.columns if re.search(r"imi", c, re.I)), None)
    nazw_guess = next((c for c in df.columns if re.search(r"nazw", c, re.I)), None)
    if imie_guess or nazw_guess:
        return "split", imie_guess, nazw_guess

    raise SystemExit("❌ Nie znalazłem kolumn z imieniem/nazwiskiem (ani pojedynczej, ani rozdzielonej).")

# ==== GŁÓWNY PROGRAM ====
def main():
    print("🏁 Rebuild ID_Zawodnika (by name) — START")

    df = norm_cols(load_csv_any(INPUT_PATH))

    # wykryj kolumny imię/nazwisko
    mode, a, b = pick_name_columns(df)

    # stary ID (jeśli jest)
    old_id_col = next((c for c in ("ID_Zawodnika", "ID", "IdZawodnika") if c in df.columns), None)
    old_ids = df[old_id_col].copy() if old_id_col else pd.Series([np.nan] * len(df))

    # zbuduj pełne imię+nazwisko i nowe ID (dla wszystkich wierszy)
    fullnames = []
    new_ids = []

    for _, row in df.iterrows():
        if mode == "split":
            first = str(row.get(a, "")).strip()
            last  = str(row.get(b, "")).strip()
            fullname = re.sub(r"\s+", " ", f"{first} {last}".strip())
        else:
            fullname = re.sub(r"\s+", " ", str(row.get(a, "")).strip())

        fullnames.append(fullname)
        new_ids.append(int(generate_player_id(fullname)))  # INT dla zgodności z kolumną INT w DB

    # zapisz w pliku wynikowym
    out = df.copy()
    out["ID_Zawodnika"] = new_ids

    # mapa stary → nowy (do weryfikacji)
    mapping = pd.DataFrame({
        "FullName": fullnames,
        "Old_ID": old_ids,
        "New_ID": new_ids,
    })

    # diagnostyka potencjalnych kolizji (ten sam New_ID dla wielu rekordów)
    dups = mapping[mapping.duplicated(["New_ID"], keep=False)].sort_values(["New_ID", "FullName"])
    if not dups.empty:
        print("⚠️ Uwaga: wykryto potencjalne kolizje New_ID (identyczne imię+nazwisko):")
        print(dups.to_string(index=False))

    out.to_csv(OUT_ROSTER, index=False, encoding="utf-8-sig")
    mapping.to_csv(OUT_MAPPING, index=False, encoding="utf-8-sig")

    print(f"✅ Zapisano: {OUT_ROSTER}  ({len(out)} rekordów)")
    print(f"✅ Zapisano: {OUT_MAPPING}  ({len(mapping)} rekordów)")
    print("\nPodgląd mapowania (10):")
    print(mapping.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
