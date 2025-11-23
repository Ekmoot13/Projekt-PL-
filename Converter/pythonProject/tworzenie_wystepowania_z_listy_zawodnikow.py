# -*- coding: utf-8 -*-
"""
tworzenie_wystepowania_z_listy_zawodnikow.py – WERSJA POPRAWIONA
ID_Regat pobierane z plików _regaty.csv (main.py), NIE liczone!
"""

import pandas as pd
from pathlib import Path
import unicodedata
import re
import hashlib
import os

# ----------- ŚCIEŻKI -----------
SRC_FILE = Path("mnt/data/występowanie/Zawodnicy_Ekstraklsa_2024.csv")
ZAWODNICY_FILE = Path("mnt/data/zawodnicy/zawodnicy.csv")
OUT_DIR = Path("mnt/data/output/wystepowanie/wystepowanie_z_listy")
MAIN_OUTPUT_REGATY_DIR = Path("mnt/data/output/main")

# ----------- KONFIGURACJA -----------
LIGA_POZIOM = "Ekstraklasa"
ROK = 2024

CLUBS_FILE = Path("mnt/data/kluby/kluby_wyciag.csv")


# ----------- NORMY ----------
def strip_accents_lower(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()


def _norm_key(s: str) -> str:
    if s is None:
        return ""
    base = str(s).strip()
    base = re.sub(r"\s+", " ", base)
    base = base.replace(" ", "")
    return base.upper()


# ----------- MAPA KLUBÓW ----------
def load_club_variant_map(path: Path) -> dict:
    if not path.exists():
        print(f"⚠ Nie znaleziono pliku z wariantami klubów: {path}")
        return {}

    df = pd.read_csv(path)

    if "Skrot" not in df.columns or "ID_wariantu_klubu" not in df.columns:
        raise ValueError("kluby_wyciag.csv musi zawierać kolumny: Skrot, ID_wariantu_klubu")

    df["Skrot_norm"] = df["Skrot"].map(_norm_key)

    club_map = dict(zip(df["Skrot_norm"], df["ID_wariantu_klubu"]))

    print(f"📚 Załadowano {len(club_map)} skrótów klubów")
    return club_map


# ----------- MAPA REGAT Z main.py ----------
def load_regaty_map(regaty_dir: Path) -> pd.DataFrame:
    rows = []

    for fname in os.listdir(regaty_dir):
        if not fname.endswith("_regaty.csv"):
            continue

        path = regaty_dir / fname
        try:
            df = pd.read_csv(path)
        except:
            continue

        required = {"ID_Regat", "Liga_Poziom", "Numer_Rundy", "Rok"}
        if not required.issubset(df.columns):
            print(f"⚠ Plik {path} pominięty – brak wymaganych kolumn")
            continue

        rows.append(df[["ID_Regat", "Liga_Poziom", "Numer_Rundy", "Rok"]])

    if not rows:
        raise RuntimeError("❌ Brak plików *_regaty.csv – odpal najpierw main.py")

    all_reg = pd.concat(rows, ignore_index=True)
    all_reg = all_reg.drop_duplicates()

    print(f"📚 Wczytano {len(all_reg)} rekordów regat")
    return all_reg


# ----------- POMOCNICZE ----------
def normalize_liga_for_filename(liga: str) -> str:
    s = unicodedata.normalize("NFKD", str(liga)).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


# ----------- MAIN ----------
def main():
    print("🏁 Start: tworzenie wystąpień z listy zawodników…")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    club_map = load_club_variant_map(CLUBS_FILE)
    regaty_df = load_regaty_map(MAIN_OUTPUT_REGATY_DIR)

    # ----------- Ładujemy zawodników ----------
    zaw = pd.read_csv(ZAWODNICY_FILE)
    zaw["full_norm"] = (
        zaw["Imie"].astype(str).str.strip() + " " +
        zaw["Nazwisko"].astype(str).str.strip()
    ).map(strip_accents_lower)
    name_to_id = dict(zip(zaw["full_norm"], zaw["ID_Zawodnika"]))

    # ----------- Ładujemy listę zawodników ----------
    df = pd.read_csv(SRC_FILE)
    df["full_norm"] = df["Zawodnik"].map(strip_accents_lower)
    df["ID_Zawodnika_new"] = df["full_norm"].map(name_to_id)

    liga_norm = normalize_liga_for_filename(LIGA_POZIOM)

    # ----------- Rekordy poprawne ----------
    valid = df[df["ID_Zawodnika_new"].notna()].copy()
    if valid.empty:
        print("❌ Brak poprawnych zawodników!")
        return

    valid["Runda"] = pd.to_numeric(valid["Runda"], errors="coerce").astype("Int64")

    # ----------- JOIN z mapą regat ----------
    merge_left = valid.copy()
    merge_left["Rok"] = ROK
    merge_left["Liga_Poziom"] = LIGA_POZIOM
    merge_left["Numer_Rundy"] = merge_left["Runda"]

    merged = merge_left.merge(
        regaty_df,
        on=["Rok", "Numer_Rundy", "Liga_Poziom"],
        how="left"
    )

    # ----------- Logowanie braków ----------
    missing_reg = merged[merged["ID_Regat"].isna()]
    if not missing_reg.empty:
        miss_path = OUT_DIR / f"brak_regat_{liga_norm}_{ROK}.csv"
        missing_reg.to_csv(miss_path, index=False, encoding="utf-8-sig")
        print(f"⚠ {len(missing_reg)} rund nie dopasowano do żadnych regat – zapisano: {miss_path}")

    ok = merged[merged["ID_Regat"].notna()].copy()

    # ----------- GENERUJEMY WYSTĘPOWANIA ----------
    rows = []
    for _, r in ok.iterrows():
        id_zaw = int(r["ID_Zawodnika_new"])
        skrot_klubu = str(r["Klub"]).strip()

        id_wariantu = club_map.get(_norm_key(skrot_klubu))

        rows.append({
            "ID_wystepowania": "",
            "ID_Zawodnika": id_zaw,
            "ID_Regat": int(r["ID_Regat"]),
            "ID_wariantu_klubu": id_wariantu,
            "WynikWRegatach": "",
            "Trening": "",
        })

    out_df = pd.DataFrame(rows)
    out_path = OUT_DIR / f"wystepowanie_{liga_norm}_{ROK}.csv"
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"✅ Zapisano: {out_path} ({len(out_df)} rekordów)")
    print(out_df.head(15).to_string(index=False))

    print("🎉 Gotowe – ID_Regat w występowaniu = ID_Regat z main.py")


if __name__ == "__main__":
    main()
