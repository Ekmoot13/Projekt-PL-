# -*- coding: utf-8 -*-
"""
tworzenieWystępowania.py (v1.1)
Poprawka: bezpieczne sortowanie nawet gdy brak rekordów + lepsza diagnostyka.
"""

import argparse
import os
import re
import hashlib
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple

# -------------------- ID generator (jak w main.py) --------------------
def generate_numeric_id(typ: str, liga_poziom: str, **params) -> str:
    param_string = f"liga_poziom={liga_poziom}|" + "|".join(f"{k}={v}" for k, v in sorted(params.items()))
    base_string = f"{typ}|{param_string}"
    hash_bytes = hashlib.sha1(base_string.encode("utf-8")).digest()
    numeric_id = int.from_bytes(hash_bytes[:4], byteorder="big")
    return f"{numeric_id % 100_000_000:08d}"

# -------------------- Parsowanie argumentów --------------------
def parse_args():
    ap = argparse.ArgumentParser(description="Budowa encji występowania na podstawie rosteru i wyników regat.")
    ap.add_argument("--wyniki", default="./mnt/data/output/all_wynikRegat.csv", help="Ścieżka do all_wynikRegat.csv")
    ap.add_argument("--zawodnicy", default="./mnt/data/zawodnicy/Zawodnicy_Ekstraklsa_2024.csv", help="Ścieżka do pliku Zawodnicy_*.csv")
    ap.add_argument("--out_csv", default="./mnt/data/wystepowania.csv", help="Ścieżka wyjściowa CSV")
    ap.add_argument("--preview", type=int, default=0, help="Ile pierwszych wierszy wypisać na stdout (0=bez podglądu)")
    ap.add_argument("--strict", action="store_true", help="Zgłaszaj błąd, gdy nie wygenerowano żadnego rekordu")
    return ap.parse_args()

# -------------------- Utilsy --------------------
def infer_liga_rok_from_filename(path: str) -> Tuple[str, int]:
    fname = os.path.basename(path)
    m = re.search(r"Zawodnicy_([A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż0-9 ]+)_([12][0-9]{3})", fname)
    if m:
        liga_poziom = m.group(1).replace("_", " ").strip()
        rok = int(m.group(2))
        return liga_poziom, rok
    return "Ekstraklasa", 2024

def load_csv_any_encoding(path: str) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "cp1250", "latin1"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise last_err

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def extract_runda_from_text(txt: str) -> Optional[int]:
    if pd.isna(txt):
        return None
    m = re.search(r"(\\d+)\\s*Runda", str(txt), re.I)
    return int(m.group(1)) if m else None

def rounds_for_row(row: pd.Series, runda_col: Optional[str], regaty_text_col: Optional[str], roundwide_cols: List[str]) -> List[int]:
    found_rounds = set()
    if runda_col and not pd.isna(row.get(runda_col, np.nan)):
        try:
            found_rounds.add(int(row[runda_col]))
        except Exception:
            pass
    if regaty_text_col and not pd.isna(row.get(regaty_text_col, np.nan)):
        r = extract_runda_from_text(row[regaty_text_col])
        if r:
            found_rounds.add(r)
    for col in roundwide_cols:
        val = row.get(col, np.nan)
        has_marker = (isinstance(val, str) and val.strip() != "") or (pd.notna(val) and not (isinstance(val, str) and val.strip() == ""))
        if has_marker:
            m = re.search(r"(\\d+)\\s*Runda", str(col), re.I)
            if m:
                found_rounds.add(int(m.group(1)))
    return sorted(list(found_rounds))

def safe_sort(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    existing = [c for c in columns if c in df.columns]
    if not existing:
        return df
    return df.sort_values(existing).reset_index(drop=True)

# -------------------- Główna logika --------------------
def build_wystepowania(wyniki_path: str, zawodnicy_path: str) -> pd.DataFrame:
    # 1) Wyniki
    wynik_df = load_csv_any_encoding(wyniki_path)
    wynik_df = norm_cols(wynik_df)
    required_cols = ["regaty", "klub", "miejsceWRegatach"]
    missing = [c for c in required_cols if c not in wynik_df.columns]
    if missing:
        raise ValueError(f"Brakuje kolumn w all_wynikRegat.csv: {missing} (mam: {list(wynik_df.columns)})")

    place_map = (
        wynik_df[["regaty", "klub", "miejsceWRegatach"]]
        .astype({"regaty": str, "klub": str})
        .groupby(["regaty", "klub"], as_index=False)
        .agg({"miejsceWRegatach": "min"})
    )
    place_key = {(r.regaty, r.klub): r.miejsceWRegatach for r in place_map.itertuples(index=False)}
    regaty_ids = set(wynik_df["regaty"].astype(str).unique().tolist())
    kluby_w_wynikach = sorted(set(wynik_df["klub"].astype(str)))

    # 2) Zawodnicy
    zaw = load_csv_any_encoding(zawodnicy_path)
    zaw = norm_cols(zaw)

    liga_poziom, rok = infer_liga_rok_from_filename(zawodnicy_path)

    name_cols_candidates = [
        ("Imię", "Nazwisko"),
        ("Imie", "Nazwisko"),
        ("FirstName", "LastName"),
        ("Imię i nazwisko", None),
        ("Zawodnik", None),
    ]
    found_name_mode = None
    for combo in name_cols_candidates:
        if combo[1] is None:
            if combo[0] in zaw.columns:
                found_name_mode = combo
                break
        else:
            if combo[0] in zaw.columns and combo[1] in zaw.columns:
                found_name_mode = combo
                break
    if found_name_mode is None:
        imie_guess = next((c for c in zaw.columns if re.search(r"imi", c, re.I)), None)
        nazw_guess = next((c for c in zaw.columns if re.search(r"nazw", c, re.I)), None)
        if imie_guess or nazw_guess:
            found_name_mode = (imie_guess, nazw_guess)

    club_col = next((c for c in ["Skrót", "Skrot", "Klub", "Zespół", "Zespol"] if c in zaw.columns), None)
    runda_col = next((c for c in ["Runda", "Numer_Rundy", "Runda #", "Numer rundy"] if c in zaw.columns), None)
    regaty_text_col = next((c for c in ["Regaty", "Runda i Regaty", "Nazwa regat"] if c in zaw.columns), None)
    roundwide_cols = [c for c in zaw.columns if re.search(r"\\b(\\d+)\\s*Runda", str(c), re.I)]
    zaw_id_col = next((c for c in ["ID_Zawodnika", "ID", "IdZawodnika"] if c in zaw.columns), None)

    def get_or_make_player_id(row: pd.Series) -> str:
        if zaw_id_col and not pd.isna(row.get(zaw_id_col, np.nan)):
            try:
                return str(int(row[zaw_id_col])).zfill(8) if len(str(int(row[zaw_id_col]))) <= 8 else str(int(row[zaw_id_col]))
            except Exception:
                return str(row[zaw_id_col]).strip()

        if found_name_mode is not None:
            imie_col, nazw_col = found_name_mode
            if nazw_col is None:
                fullname = str(row.get(imie_col, "")).strip()
            else:
                fullname = f"{str(row.get(imie_col, '')).strip()} {str(row.get(nazw_col, '')).strip()}".strip()
        else:
            fullname = str(row.get("Zawodnik", "")).strip()

        skrot_klubu = str(row.get(club_col, "")).strip() if club_col else ""
        return generate_numeric_id("zawodnik", liga_poziom, rok=rok, klub=skrot_klubu, nazwisko_imie=fullname)

    runda_to_regaty: Dict[int, str] = {}
    for nr in range(1, 11):
        reg_id = generate_numeric_id("regaty", liga_poziom, rok=rok, runda=nr)
        if reg_id in regaty_ids:
            runda_to_regaty[nr] = reg_id

    rows: List[Dict] = []
    unresolved_rows: List[int] = []

    for idx, row in zaw.iterrows():
        skrot_klubu = str(row.get(club_col, "")).strip() if club_col else ""
        player_id = get_or_make_player_id(row)
        rounds = rounds_for_row(row, runda_col, regaty_text_col, roundwide_cols)

        if not rounds:
            candidate_regaty = [reg for (reg, klub) in place_key.keys() if klub == skrot_klubu]
            if not candidate_regaty:
                unresolved_rows.append(idx)
                continue
            for runda, reg_id in runda_to_regaty.items():
                if reg_id in candidate_regaty:
                    rounds.append(runda)
            rounds = sorted(set(rounds))

        for runda in rounds:
            reg_id = runda_to_regaty.get(runda)
            if not reg_id:
                continue
            miejsce = place_key.get((reg_id, skrot_klubu))
            rows.append({
                "ID_wystepowania": generate_numeric_id("wystepowanie", liga_poziom, rok=rok, runda=runda, zawodnik=player_id, klub=skrot_klubu),
                "ID_Zawodnika": player_id,
                "ID_Regat": reg_id,
                "Skrót": skrot_klubu,
                "Runda": runda,
                "Miejsce_w_regatach_klubu": miejsce,
                "Liga": liga_poziom,
                "Rok": rok,
            })

    expected_cols = ["ID_wystepowania","ID_Zawodnika","ID_Regat","Skrót","Runda","Miejsce_w_regatach_klubu","Liga","Rok"]
    df_out = pd.DataFrame(rows, columns=expected_cols)
    df_out = safe_sort(df_out, ["Rok", "Liga", "Runda", "Skrót", "ID_Zawodnika"])

    # Info diagnostyczne (drukowane w main)
    df_out.attrs["diagnostics"] = {
        "liczba_wierszy_rosteru": int(len(zaw)),
        "liczba_wierszy_wystepowan": int(len(df_out)),
        "nieprzypisane_wiersze_rosteru": int(len(unresolved_rows)),
        "rundy_wykryte": sorted(list(runda_to_regaty.keys())),
        "wykryty_club_col": club_col,
        "name_mode": (found_name_mode if found_name_mode else "NIE ZNALEZIONO"),
        "liczba_regat_w_wynikach": int(len(regaty_ids)),
        "liczba_klubow_w_wynikach": int(len(kluby_w_wynikach)),
    }
    return df_out

def main():
    args = parse_args()
    # Ostrzeżenie, jeśli ktoś ma inne domyślne ścieżki w projekcie
    if not os.path.exists(args.wyniki):
        print(f"⚠️  Nie znaleziono pliku --wyniki: {args.wyniki}")
    if not os.path.exists(args.zawodnicy):
        print(f"⚠️  Nie znaleziono pliku --zawodnicy: {args.zawodnicy}")
    df = build_wystepowania(args.wyniki, args.zawodnicy)
    out_path = args.out_csv
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    diags = df.attrs.get("diagnostics", {})
    print("✅ Zapisano:", out_path)
    print("📊 Diagnoza:", diags)
    if args.preview > 0:
        print("🔎 Podgląd:")
        print(df.head(args.preview).to_string(index=False))

    if args.strict and len(df) == 0:
        raise SystemExit("❌ Strict mode: wygenerowano 0 rekordów. Sprawdź ścieżki wejściowe i dopasowanie kolumn.")

if __name__ == "__main__":
    main()
