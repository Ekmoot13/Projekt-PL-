# -*- coding: utf-8 -*-
"""
export_wystepowania_demo10.py
----------------------------------
Skrypt do wygenerowania pliku CSV z encją „występowanie w regatach”
zgodnie z tabelą:
    liga_Wystepowanie_w_regatach
z deterministycznym systemem ID (zgodnym z main.py).

✅ Tworzy dwa pliki:
 - wystepowanie_w_regatach_demo10.csv — gotowy do importu do bazy
 - _QC_wystepowanie_with_wynik_demo10.csv — kontrolny z WynikWRegatach

Wystarczy odpalić w PyCharmie (Run ▶️)
"""

import os
import re
import hashlib
import pandas as pd
import numpy as np

# === USTAWIENIA ===
WYNIKI_PATH = "./mnt/data/output/all_wynikRegat.csv"
ZAWODNICY_PATH = "./mnt/data/zawodnicy/Zawodnicy_Ekstraklsa_2024.csv"
OUT_CSV = "./mnt/data/datawystepowanie_w_regatach_demo10.csv"
OUT_QC = "./mnt/data/_QC_wystepowanie_with_wynik_demo10.csv"
LIMIT = 10  # ile rekordów demo wygenerować

# === FUNKCJE POMOCNICZE ===
def generate_numeric_id(typ: str, liga_poziom: str, **params) -> str:
    """
    Funkcja do generowania deterministycznego ID (8 cyfr)
    Zgodna z main.py
    """
    param_string = f"liga_poziom={liga_poziom}|" + "|".join(
        f"{k}={v}" for k, v in sorted(params.items())
    )
    base_string = f"{typ}|{param_string}"
    hash_bytes = hashlib.sha1(base_string.encode()).digest()
    numeric_id = int.from_bytes(hash_bytes[:4], byteorder="big")
    return f"{numeric_id % 100_000_000:08d}"

def load_csv_any(path: str) -> pd.DataFrame:
    """Ładuje CSV w różnych kodowaniach"""
    for enc in ["utf-8-sig", "utf-8", "cp1250", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return pd.read_csv(path)

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def infer_liga_rok_from_filename(path: str):
    """Wydobywa poziom ligi i rok z nazwy pliku"""
    fname = os.path.basename(path)
    m = re.search(r"Zawodnicy_([A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż0-9 ]+)_([12][0-9]{3})", fname)
    if m:
        return m.group(1).replace("_", " ").strip(), int(m.group(2))
    return "Ekstraklasa", 2024

# === GŁÓWNY PROGRAM ===
def main():
    print("🏁 Start generowania występowania (demo 10 rekordów)")

    # 1️⃣ Wczytanie danych
    wyn = norm_cols(load_csv_any(WYNIKI_PATH))
    rost = norm_cols(load_csv_any(ZAWODNICY_PATH))

    # 2️⃣ Weryfikacja kolumn
    if not {"regaty", "klub", "miejsceWRegatach"}.issubset(wyn.columns):
        raise SystemExit("❌ Brakuje kolumn w all_wynikRegat.csv: regaty, klub, miejsceWRegatach")

    club_col = next((c for c in ["Skrót","Skrot","Klub","Zespół","Zespol"] if c in rost.columns), None)
    id_col = next((c for c in ["ID_Zawodnika","ID","IdZawodnika"] if c in rost.columns), None)
    if not club_col:
        raise SystemExit("❌ Brak kolumny skrótu klubu (Skrót/Skrot/Klub) w rosterze")

    name_mode = None
    for a,b in [("Imię","Nazwisko"),("Imie","Nazwisko"),("FirstName","LastName"),("Imię i nazwisko",None),("Zawodnik",None)]:
        if b is None and a in rost.columns:
            name_mode = (a,b); break
        if b is not None and a in rost.columns and b in rost.columns:
            name_mode = (a,b); break

    wyn["regaty"] = wyn["regaty"].astype(str)
    wyn["klub"] = wyn["klub"].astype(str)
    place = {(r.regaty, r.klub): r.miejsceWRegatach for r in wyn[["regaty","klub","miejsceWRegatach"]].itertuples(index=False)}
    reg_ids = set(wyn["regaty"].astype(str).unique())

    # 3️⃣ Liga i rok
    liga_file, rok_file = infer_liga_rok_from_filename(ZAWODNICY_PATH)
    liga = "Ekstraklasa" if str(liga_file).lower() == "ekstraklsa" else liga_file
    rok = rok_file

    # 4️⃣ Generacja rekordów
    rows_out, rows_qc = [], []
    emitted = 0

    for _, row in rost.iterrows():
        if emitted >= LIMIT:
            break
        skrot = str(row.get(club_col, "")).strip()
        if not skrot:
            continue

        # Imię i nazwisko
        if name_mode:
            a,b = name_mode
            if b is None:
                fullname = str(row.get(a,"")).strip()
            else:
                fullname = f"{str(row.get(a,''))} {str(row.get(b,''))}".strip()
        else:
            fullname = str(row.get("Zawodnik","")).strip()

        # ID zawodnika
        if id_col and pd.notna(row.get(id_col, np.nan)):
            try:
                zawodnik_id = str(int(row[id_col])).zfill(8)
            except Exception:
                zawodnik_id = str(row[id_col]).strip()
        else:
            zawodnik_id = generate_numeric_id("zawodnik", liga, rok=rok, klub=skrot, nazwisko_imie=fullname)

        # Iteracja po rundach (1..12)
        for nr in range(1, 13):
            if emitted >= LIMIT:
                break
            rid = generate_numeric_id("regaty", liga, rok=rok, runda=nr)
            if rid not in reg_ids:
                continue
            if (rid, skrot) not in place:
                continue

            wyst_id = generate_numeric_id("wystepowanie", liga, rok=rok, runda=nr, zawodnik=zawodnik_id, klub=skrot)
            wynik = place[(rid, skrot)]
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
                "WynikWRegatach": wynik
            })
            emitted += 1

    # 5️⃣ Zapis
    df_out = pd.DataFrame(rows_out)
    df_qc = pd.DataFrame(rows_qc)
    df_out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    df_qc.to_csv(OUT_QC, index=False, encoding="utf-8-sig")

    print(f"✅ Zapisano {len(df_out)} rekordów do {OUT_CSV}")
    print(f"ℹ️ Plik kontrolny (QC): {OUT_QC}")

    print("\nPodgląd 10 rekordów:")
    print(df_out.head(10).to_string(index=False))

# === START ===
if __name__ == "__main__":
    main()
