# -*- coding: utf-8 -*-
"""
export_wystepowania_from_PLZ2025.py – wersja z kanonicznym ID_Regat

Wejście: PLZ_uczestnicy_2025.xlsx
Wyjście: wystepowanie_<liga>_2025.csv z:
  ID_wystepowania, ID_Zawodnika, ID_Regat, Skrot, Trening
"""

import pandas as pd
import hashlib
import re
import unicodedata
from pathlib import Path

SRC_PLZ2025 = Path("mnt/data/występowanie/PLZ_uczestnicy_2025.xlsx")
OUT_DIR = Path("mnt/data/output/wystepowanie/xlsx")


def strip_accents_lower(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()


def generate_player_id_only_name(fullname: str) -> str:
    norm = strip_accents_lower(fullname)
    base = f"zawodnik|name_norm={norm}"
    h = hashlib.sha1(base.encode()).digest()
    num = int.from_bytes(h[:4], "big") % 100_000_000
    return f"{num:08d}"


def generate_numeric_id(typ: str, liga_poziom: str, **params) -> str:
    """
    KANONICZNE ID jak w main.py:
      sha1(f"{typ}|liga_poziom={liga}|k1=v1|k2=v2|...") -> 8 cyfr

    Dla regat:
      generate_numeric_id("regaty", liga_poziom, rok=rok, runda=runda)
    """
    param_string = f"liga_poziom={liga_poziom}|" + "|".join(
        f"{k}={v}" for k, v in sorted(params.items())
    )
    base_string = f"{typ}|{param_string}"
    h = hashlib.sha1(base_string.encode()).digest()
    num = int.from_bytes(h[:4], "big") % 100_000_000
    return f"{num:08d}"


def extract_tag(klub_cell: str) -> str:
    s = str(klub_cell or "").strip()
    m = re.search(r"\(([^)]+)\)\s*$", s)
    if m:
        return m.group(1).strip()
    tokens = re.split(r"[\s,/|-]+", s)
    for tok in reversed(tokens):
        if tok.isupper() and 2 <= len(tok) <= 4:
            return tok
    return s[:10].upper()


def liga_from_regaty_cell(cell: str) -> str:
    t = str(cell or "").strip().lower()
    if "młodzież" in t or "mlodziez" in t:
        return "Youth"
    if "regional" in t:
        return "Ligi Regionalne – Finał"
    if "ekstra" in t:
        return "Ekstraklasa"
    if ("1" in t and "liga" in t) or "i liga" in t:
        return "1 Liga"
    return "Ekstraklasa"


def normalize_liga_for_filename(liga: str) -> str:
    s = str(liga or "").strip()
    s_norm = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s_norm = s_norm.lower()
    s_norm = re.sub(r"[–\-]+", " ", s_norm)
    s_norm = re.sub(r"[^a-z0-9]+", " ", s_norm)
    s_norm = re.sub(r"\s+", "_", s_norm).strip("_")
    return s_norm


def main():
    print("🏁 Generuję pliki występowania z uczestników 2025 (kanoniczne ID_Regat)...")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    ucz = pd.read_excel(SRC_PLZ2025)
    ucz.columns = [c.strip().lower() for c in ucz.columns]

    imie_col = "imię" if "imię" in ucz.columns else "imie"
    nazw_col = "nazwisko"
    runda_col = "runda"
    klub_col = "klub"
    regaty_col = "regaty"

    wyst = pd.DataFrame({
        "Zawodnik": (ucz[imie_col].astype(str).str.strip() + " " +
                     ucz[nazw_col].astype(str).str.strip()).str.replace(r"\s+", " ", regex=True),
        "ID_klubu": ucz[klub_col].apply(extract_tag),
        "rok": 2025,
        "regaty": ucz[regaty_col].astype(str),
        "numer_rundy": pd.to_numeric(ucz[runda_col], errors="coerce").astype("Int64"),
        "poziom_ligi": ucz[regaty_col].apply(liga_from_regaty_cell)
    }).dropna(subset=["Zawodnik", "ID_klubu", "numer_rundy"])

    rows = []
    for _, r in wyst.iterrows():
        full_name = str(r["Zawodnik"]).strip()
        skrot_klubu = str(r["ID_klubu"]).strip()
        rok = int(r["rok"])
        poziom_ligi = str(r["poziom_ligi"]).strip()
        runda = int(r["numer_rundy"])

        # 🔥 KANONICZNE ID_Regat – BEZ miasta:
        id_regat = int(generate_numeric_id(
            "regaty", poziom_ligi,
            rok=rok, runda=runda
        ))
        id_zaw = int(generate_player_id_only_name(full_name))
        id_wyst = int(generate_numeric_id(
            "wystepowanie", poziom_ligi,
            rok=rok, runda=runda, regaty=id_regat,
            zawodnik=id_zaw, klub=skrot_klubu
        ))

        rows.append({
            "ID_wystepowania": "",
            "ID_Zawodnika": id_zaw,
            "ID_Regat": id_regat,
            "Skrot": skrot_klubu,
            "WynikWRegatach": "",
            "Trening": "",
        })

    if not rows:
        print("⚠ Brak wierszy do zapisania.")
        return

    out_df = pd.DataFrame(rows)

    df_save = out_df[["ID_wystepowania", "ID_Zawodnika", "ID_Regat", "Skrot", "WynikWRegatach", "Trening"]]

    out_path = OUT_DIR / "wystepowanie_all.csv"
    df_save.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"✅ Zapisano jeden plik: {out_path} ({len(df_save)} rekordów)")
    print(df_save.head(15).to_string(index=False))

if __name__ == "__main__":
    main()
