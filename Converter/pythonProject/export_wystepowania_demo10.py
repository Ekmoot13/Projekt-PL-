# -*- coding: utf-8 -*-
"""
export_wystepowania_demo10_by_name.py
----------------------------------
Generuje 10 rekordów encji 'występowanie' z deterministycznym ID_Zawodnika
liczonym WYŁĄCZNIE z imienia i nazwiska.

Wejście:
  - all_wynikRegat.csv
  - Zawodnicy_Ekstraklsa_2024.csv   (lub Twój plik rosteru)

Wyjście:
  - wystepowanie_w_regatach_demo10.csv          (do importu)
  - _QC_wystepowanie_with_wynik_demo10.csv      (kontrolny z WynikWRegatach)
"""

import os, re, hashlib, pandas as pd, numpy as np, unicodedata

WYNIKI_PATH = "./mnt/data/output/all_wynikRegat.csv"
ZAWODNICY_PATH = "./mnt/data/zawodnicy/Zawodnicy_Ekstraklsa_2024.csv"
OUT_CSV = "wystepowanie_w_regatach_demo10.csv"
OUT_QC  = "_QC_wystepowanie_with_wynik_demo10.csv"
LIMIT = 10

def strip_accents_lower(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    return re.sub(r"\s+", " ", s).lower()

def generate_player_id(fullname: str) -> str:
    norm = strip_accents_lower(fullname)
    base = f"zawodnik|name_norm={norm}"
    h = hashlib.sha1(base.encode()).digest()
    return f"{int.from_bytes(h[:4],'big') % 100_000_000:08d}"

def generate_numeric_id(typ: str, liga_poziom: str, **params) -> str:
    param_string = f"liga_poziom={liga_poziom}|" + "|".join(f"{k}={v}" for k, v in sorted(params.items()))
    base_string = f"{typ}|{param_string}"
    h = hashlib.sha1(base_string.encode()).digest()
    return f"{int.from_bytes(h[:4],'big') % 100_000_000:08d}"

def load_csv_any(path: str) -> pd.DataFrame:
    for enc in ["utf-8-sig","utf-8","cp1250","latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return pd.read_csv(path)

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def infer_liga_rok_from_filename(path: str):
    fname = os.path.basename(path)
    m = re.search(r"Zawodnicy_([A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż0-9 ]+)_([12][0-9]{3})", fname)
    if m:
        return m.group(1).replace("_"," ").strip(), int(m.group(2))
    return "Ekstraklasa", 2024

def pick_name_columns(df: pd.DataFrame):
    candidates = [
        ("split","Imię","Nazwisko"),
        ("split","Imie","Nazwisko"),
        ("split","FirstName","LastName"),
        ("single","Imię i nazwisko", None),
        ("single","Zawodnik", None),
    ]
    for mode,a,b in candidates:
        if mode=="split" and a in df.columns and b in df.columns:
            return mode,a,b
        if mode=="single" and a in df.columns:
            return mode,a,b
    # heurystyka
    imie_guess = next((c for c in df.columns if re.search(r"imi", c, re.I)), None)
    nazw_guess = next((c for c in df.columns if re.search(r"nazw", c, re.I)), None)
    if imie_guess or nazw_guess:
        return "split", imie_guess, nazw_guess
    return None, None, None

def main():
    wyn = norm_cols(load_csv_any(WYNIKI_PATH))
    rost = norm_cols(load_csv_any(ZAWODNICY_PATH))
    if not {"regaty","klub","miejsceWRegatach"}.issubset(wyn.columns):
        raise SystemExit("Brakuje kolumn w all_wynikRegat.csv (regaty, klub, miejsceWRegatach).")

    club_col = next((c for c in ["Skrót","Skrot","Klub","Zespół","Zespol"] if c in rost.columns), None)
    if not club_col:
        raise SystemExit("Brak kolumny skrótu klubu w rosterze.")
    mode, a, b = pick_name_columns(rost)
    if not mode:
        raise SystemExit("Brak kolumn z imieniem/nazwiskiem w rosterze.")

    # zestawy pomocnicze
    wyn["regaty"] = wyn["regaty"].astype(str); wyn["klub"] = wyn["klub"].astype(str)
    place = {(r.regaty, r.klub): r.miejsceWRegatach for r in wyn[["regaty","klub","miejsceWRegatach"]].itertuples(index=False)}
    reg_ids = set(wyn["regaty"].astype(str).unique())

    liga_file, rok_file = infer_liga_rok_from_filename(ZAWODNICY_PATH)
    liga = "Ekstraklasa" if str(liga_file).lower()=="ekstraklsa" else liga_file
    rok = rok_file

    rows_out, rows_qc = [], []
    emitted = 0

    for _, row in rost.iterrows():
        if emitted >= LIMIT:
            break
        skrot = str(row.get(club_col, "")).strip()
        if not skrot:
            continue

        # fullname
        if mode == "split":
            fullname = f"{row.get(a,'')}".strip() + " " + f"{row.get(b,'')}".strip()
        else:
            fullname = f"{row.get(a,'')}".strip()
        fullname = re.sub(r"\s+", " ", fullname).strip()

        # NOWE: ID_Zawodnika wyłącznie z imienia+nazwiska
        zawodnik_id = generate_player_id(fullname)

        # iteruj po rundach (1..12), ale tylko takich, które istnieją w wynikach i mają miejsce dla klubu
        for nr in range(1, 12+1):
            if emitted >= LIMIT:
                break
            rid = generate_numeric_id("regaty", liga, rok=rok, runda=nr)
            if rid not in reg_ids:
                continue
            if (rid, skrot) not in place:
                continue
            wyst_id = generate_numeric_id("wystepowanie", liga, rok=rok, runda=nr, zawodnik=zawodnik_id, klub=skrot)

            rows_out.append({
                "ID_wystepowania": int(wyst_id),
                "ID_Zawodnika": int(zawodnik_id),
                "ID_Regat": int(rid),
                "Skrot": skrot,
                "Trening": ""
            })
            rows_qc.append({
                "ID_wystepowania": int(wyst_id),
                "ID_Zawodnika": int(zawodnik_id),
                "ID_Regat": int(rid),
                "Skrot": skrot,
                "WynikWRegatach": place[(rid, skrot)]
            })
            emitted += 1

    pd.DataFrame(rows_out).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    pd.DataFrame(rows_qc).to_csv(OUT_QC,  index=False, encoding="utf-8-sig")
    print(f"✅ Zapisano {OUT_CSV} ({len(rows_out)} rekordów)")
    print(f"ℹ️ QC: {OUT_QC} ({len(rows_qc)} rekordów)")
    print("\nPodgląd:")
    print(pd.DataFrame(rows_out).head(10).to_string(index=False))

if __name__ == "__main__":
    main()
