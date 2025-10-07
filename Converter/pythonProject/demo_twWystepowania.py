
# -*- coding: utf-8 -*-
import argparse, os, re, hashlib, pandas as pd, numpy as np

def generate_numeric_id(typ: str, liga_poziom: str, **params) -> str:
    param_string = f"liga_poziom={liga_poziom}|" + "|".join(f"{k}={v}" for k, v in sorted(params.items()))
    base_string = f"{typ}|{param_string}"
    import hashlib as _h
    hash_bytes = _h.sha1(base_string.encode()).digest()
    numeric_id = int.from_bytes(hash_bytes[:4], byteorder='big')
    return f"{numeric_id % 100_000_000:08d}"

def infer_liga_rok_from_filename(path: str):
    fname = os.path.basename(path)
    m = re.search(r"Zawodnicy_([A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż0-9 ]+)_([12][0-9]{3})", fname)
    if m:
        return m.group(1).replace("_"," ").strip(), int(m.group(2))
    return "Ekstraklasa", 2024

def load_csv_any(path: str):
    import pandas as _pd
    for enc in ["utf-8-sig","utf-8","cp1250","latin1"]:
        try:
            return _pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return _pd.read_csv(path)

def norm_cols(df):
    df.columns = [str(c).strip() for c in df.columns]
    return df

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wyniki", default="./mnt/data/output/all_wynikRegat.csv")
    ap.add_argument("--zawodnicy", default="./mnt/data/zawodnicy/Zawodnicy_Ekstraklsa_2024.csv")
    ap.add_argument("--variant", choices=["A","B"], default="A")
    ap.add_argument("--liga", default=None)
    ap.add_argument("--rok", type=int, default=None)
    ap.add_argument("--limit", type=int, default=10)
    return ap.parse_args()

def main():
    args = parse_args()
    wyn = norm_cols(load_csv_any(args.wyniki))
    if not set(["regaty","klub","miejsceWRegatach"]).issubset(wyn.columns):
        print("Brakuje kolumn w all_wynikRegat.csv"); return
    wyn["regaty"] = wyn["regaty"].astype(str); wyn["klub"] = wyn["klub"].astype(str)
    place = {(r.regaty, r.klub): r.miejsceWRegatach for r in wyn[["regaty","klub","miejsceWRegatach"]].itertuples(index=False)}
    reg_ids = set(wyn["regaty"].astype(str).unique())
    rost = norm_cols(load_csv_any(args.zawodnicy))

    # heurystyka kolumn
    club_col = next((c for c in ["Skrót","Skrot","Klub","Zespół","Zespol"] if c in rost.columns), None)
    if not club_col: print("Brak kolumny Skrót/Skrot/Klub w rosterze"); return
    id_col = next((c for c in ["ID_Zawodnika","ID","IdZawodnika"] if c in rost.columns), None)

    name_mode=None
    for a,b in [("Imię","Nazwisko"),("Imie","Nazwisko"),("FirstName","LastName"),("Imię i nazwisko",None),("Zawodnik",None)]:
        if b is None and a in rost.columns: name_mode=(a,b); break
        if b is not None and a in rost.columns and b in rost.columns: name_mode=(a,b); break

    liga_file, rok_file = infer_liga_rok_from_filename(args.zawodnicy)
    liga = args.liga or liga_file
    if str(liga).lower()=='ekstraklsa': liga='Ekstraklasa'
    rok = args.rok or rok_file

    out_rows=[]; shown=0
    for _, row in rost.iterrows():
        if shown>=args.limit: break
        skrot = str(row.get(club_col,"")).strip()
        if not skrot: continue

        # Imię/Nazwisko do logu
        if name_mode:
            a,b = name_mode
            if b is None:
                imie = ""; nazwisko = str(row.get(a,"")).strip()
                fullname = nazwisko
            else:
                imie = str(row.get(a,"")).strip(); nazwisko = str(row.get(b,"")).strip()
                fullname = f"{imie} {nazwisko}".strip()
        else:
            imie = ""; nazwisko = str(row.get("Zawodnik","")).strip()
            fullname = nazwisko

        # wariant A/B
        if args.variant=="B":
            liga_use = row.get("poziom_ligi", liga)
            rok_use = int(row.get("rok", rok))
            runda_val = row.get("Runda", row.get("numer rundy", None))
            candidate_rundy = [int(runda_val)] if pd.notna(runda_val) else list(range(1,13))
        else:
            liga_use = liga; rok_use = rok
            runda_val = row.get("Runda", None)
            candidate_rundy = [int(runda_val)] if pd.notna(runda_val) else list(range(1,13))

        # ID zawodnika
        if id_col and pd.notna(row.get(id_col, np.nan)):
            try:
                zawodnik_id = str(int(row[id_col])).zfill(8) if len(str(int(row[id_col])))<=8 else str(int(row[id_col]))
            except Exception:
                zawodnik_id = str(row[id_col]).strip()
        else:
            zawodnik_id = generate_numeric_id("zawodnik", str(liga_use), rok=rok_use, klub=skrot, nazwisko_imie=fullname)

        # iteracja po rundach
        for nr in candidate_rundy:
            try:
                rid = generate_numeric_id("regaty", str(liga_use), rok=rok_use, runda=int(nr))
            except Exception:
                continue
            if rid not in reg_ids: continue
            if (rid, skrot) not in place: continue
            miejsce = place[(rid, skrot)]
            wyst_id = generate_numeric_id("wystepowanie", str(liga_use), rok=rok_use, runda=int(nr), zawodnik=zawodnik_id, klub=skrot)

            # WYPIS PARAMETRÓW
            print(
                f"Zawodnik={imie} {nazwisko}; Klub={skrot}; Liga={liga_use}; Rok={rok_use}; Runda={nr}; "
                f"ID_Regat={rid}; WynikWRegatach={miejsce}; ID_Zawodnika={zawodnik_id}; ID_wystepowania={wyst_id}"
            )

            shown+=1
            if shown>=args.limit: break

    if shown==0:
        print("❌ Demo nie znalazło pasujących rekordów. Sprawdź ligę/rok/rundy lub nazwy klubów.")

if __name__=='__main__':
    main()
