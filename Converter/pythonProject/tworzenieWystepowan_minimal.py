# -*- coding: utf-8 -*-
"""
tworzenieWystepowan_minimal.py
--------------------------------
Wersja PROSTA: dla każdego zawodnika z rosteru sprawdza WSZYSTKIE regaty (rundy)
wykryte w all_wynikRegat.csv dla jego klubu i tworzy encję występowania.
Nie czytamy żadnych kolumn typu "Runda" z rosteru — wszystko bierzemy z wyników.

ID liczone dokładnie jak w main.py.
"""

import argparse
import os
import re
import hashlib
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple

# ===== ID z main.py =====
def generate_numeric_id(typ: str, liga_poziom: str, **params) -> str:
    param_string = f"liga_poziom={liga_poziom}|" + "|".join(f"{k}={v}" for k, v in sorted(params.items()))
    base_string = f"{typ}|{param_string}"
    hash_bytes = hashlib.sha1(base_string.encode()).digest()
    numeric_id = int.from_bytes(hash_bytes[:4], byteorder='big')
    return f"{numeric_id % 100_000_000:08d}"

def infer_liga_rok_from_filename(path: str) -> Tuple[str, int]:
    fname = os.path.basename(path)
    m = re.search(r"Zawodnicy_([A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż0-9 ]+)_([12][0-9]{3})", fname)
    if m:
        liga_poziom = m.group(1).replace("_", " ").strip()
        rok = int(m.group(2))
        return liga_poziom, rok
    return "Ekstraklasa", 2024

def load_csv_any_encoding(path: str) -> pd.DataFrame:
    for enc in ["utf-8-sig","utf-8","cp1250","latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    # ostatnia próba bez enc
    return pd.read_csv(path)

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wyniki", default="./mnt/data/output/all_wynikRegat.csv")
    ap.add_argument("--zawodnicy", default="./mnt/data/zawodnicy/Zawodnicy_Ekstraklsa_2024.csv")
    ap.add_argument("--out_csv", default="./mnt/data/wystepowania.csv")
    ap.add_argument("--preview", type=int, default=0)
    return ap.parse_args()

def main():
    args = parse_args()
    wyn = load_csv_any_encoding(args.wyniki); wyn = norm_cols(wyn)
    req_wyn = ["regaty","klub","miejsceWRegatach"]
    miss = [c for c in req_wyn if c not in wyn.columns]
    if miss: raise SystemExit(f"Brakuje w all_wynikRegat: {miss}")

    roster = load_csv_any_encoding(args.zawodnicy); roster = norm_cols(roster)
    liga, rok = infer_liga_rok_from_filename(args.zawodnicy)

    # kolumny pomocnicze
    club_col = next((c for c in ["Skrót","Skrot","Klub","Zespół","Zespol"] if c in roster.columns), None)
    if not club_col: raise SystemExit("Nie znaleziono kolumny skrótu klubu w rosterze.")
    id_col = next((c for c in ["ID_Zawodnika","ID","IdZawodnika"] if c in roster.columns), None)
    name_mode = None
    for pair in [("Imię","Nazwisko"),("Imie","Nazwisko"),("FirstName","LastName"),("Imię i nazwisko",None),("Zawodnik",None)]:
        if pair[1] is None and pair[0] in roster.columns:
            name_mode = pair; break
        if pair[1] is not None and pair[0] in roster.columns and pair[1] in roster.columns:
            name_mode = pair; break

    # map: (regaty, klub) -> miejsce
    wyn["regaty"] = wyn["regaty"].astype(str); wyn["klub"] = wyn["klub"].astype(str)
    place = {(r.regaty, r.klub): r.miejsceWRegatach for r in wyn[["regaty","klub","miejsceWRegatach"]].itertuples(index=False)}

    # wykryj rundy tej ligi/roku: generujemy ID dla runda=1..12 i sprawdzamy czy jest w wynikach
    candidate_rundy = []
    reg_ids = set(wyn["regaty"].astype(str).unique())
    for nr in range(1, 13):
        rid = generate_numeric_id("regaty", liga, rok=rok, runda=nr)
        if rid in reg_ids:
            candidate_rundy.append((nr, rid))

    rows = []
    for _, r in roster.iterrows():
        skrot = str(r.get(club_col, "")).strip()
        if not skrot: 
            continue

        # ID zawodnika (albo z pliku, albo deterministyczny z imienia+nazwiska)
        if id_col and pd.notna(r.get(id_col, np.nan)):
            try:
                zawodnik_id = str(int(r[id_col])).zfill(8) if len(str(int(r[id_col]))) <= 8 else str(int(r[id_col]))
            except Exception:
                zawodnik_id = str(r[id_col]).strip()
        else:
            if name_mode:
                a,b = name_mode
                if b is None:
                    fullname = str(r.get(a,"")).strip()
                else:
                    fullname = f"{str(r.get(a,''))} {str(r.get(b,''))}".strip()
            else:
                fullname = ""
            zawodnik_id = generate_numeric_id("zawodnik", liga, rok=rok, klub=skrot, nazwisko_imie=fullname)

        # dla wszystkich rund, w których klub ma wynik => tworzymy występowanie
        for nr, rid in candidate_rundy:
            miejsce = place.get((rid, skrot))
            if pd.isna(miejsce) or (rid, skrot) not in place:
                continue
            wyst_id = generate_numeric_id("wystepowanie", liga, rok=rok, runda=nr, zawodnik=zawodnik_id, klub=skrot)
            rows.append({
                "ID_wystepowania": wyst_id,
                "ID_Zawodnika": zawodnik_id,
                "ID_Regat": rid,
                "Runda": nr,
                "Skrót": skrot,
                "Miejsce_w_regatach_klubu": miejsce,
                "Liga": liga,
                "Rok": rok
            })

    out = pd.DataFrame(rows, columns=["ID_wystepowania","ID_Zawodnika","ID_Regat","Runda","Skrót","Miejsce_w_regatach_klubu","Liga","Rok"])
    out.to_csv(args.out_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Zapisano {len(out)} rekordów do: {args.out_csv}")
    print(f"ℹ️  Wykryte rundy: {[nr for nr,_ in candidate_rundy]}")

    if args.preview:
        print(out.head(args.preview).to_string(index=False))

if __name__ == "__main__":
    main()
