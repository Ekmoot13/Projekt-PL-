# -*- coding: utf-8 -*-
"""
tworzenie_wystepowania_z_ankiety.py – WERSJA Z MAPĄ KLUBÓW

✔ NIE liczy ID_wariantu_klubu z nazwy.
✔ Bierze ID_wariantu_klubu z kluby_wyciag.csv po samym skrócie.
✔ Pomija wiersze bez ID_Zawodnika.
✔ Dodaje kolumnę ID_wystepowania (pustą).
"""

from pathlib import Path
import pandas as pd
import re

BASE_DIR = Path("mnt/data")

IN_FILE_CANDIDATES = [
    BASE_DIR / "występowanie" / "wystepowania_z_ankiety_completed.csv",
    BASE_DIR / "wystepowania_z_ankiety_completed.csv",
]

OUT_DIR = BASE_DIR / "output" / "wystepowanie" / "ankieta"
OUT_FILE = OUT_DIR / "wystepowania_z_ankiety_all.csv"
OUT_MISSING = OUT_DIR / "wystepowania_z_ankiety_missing_kluby.csv"

CLUBS_FILE = BASE_DIR / "kluby" / "kluby_wyciag.csv"


def _norm_key(s: str) -> str:
    """
    Normalizacja skrótu klubu:
    - trim
    - redukcja wielu spacji
    - usunięcie WSZYSTKICH spacji
    - UPPER
    """
    if s is None:
        return ""
    base = str(s).strip()
    base = re.sub(r"\s+", " ", base)
    base = base.replace(" ", "")
    return base.upper()


def load_club_variant_map(path: Path) -> dict:
    """
    Ładuje mapę: znormalizowany skrót -> ID_wariantu_klubu
    z pliku kluby_wyciag.csv (wygenerowanego przez kluby.py).
    """
    if not path.exists():
        print(f"⚠ Nie znaleziono pliku z wariantami klubów: {path}")
        return {}

    df = pd.read_csv(path)
    if "Skrot" not in df.columns or "ID_wariantu_klubu" not in df.columns:
        print("⚠ kluby_wyciag.csv musi mieć kolumny: Skrot, ID_wariantu_klubu")
        return {}

    df["Skrot_norm"] = df["Skrot"].map(_norm_key)

    club_map = {}
    for _, r in df.iterrows():
        key = r["Skrot_norm"]
        if key not in club_map:
            club_map[key] = int(r["ID_wariantu_klubu"])

    print(f"📚 Załadowano mapę klubów: {len(club_map)} skrótów")
    return club_map


def load_input() -> pd.DataFrame:
    for path in IN_FILE_CANDIDATES:
        if path.exists():
            print(f"📥 Czytam wejście z: {path}")
            return pd.read_csv(path, encoding="utf-8")
    raise FileNotFoundError(
        "Brak pliku 'wystepowania_z_ankiety_completed.csv' ani w 'mnt/data', "
        "ani w 'mnt/data/występowanie'."
    )


def prepare_for_db(df: pd.DataFrame, club_map: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()

    required = ["ID_Zawodnika", "ID_Regat", "Skrot", "WynikWRegatach"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Brakuje obowiązkowej kolumny: {col}")

    df["ID_Zawodnika"] = pd.to_numeric(df["ID_Zawodnika"], errors="coerce").astype("Int64")
    before = len(df)
    df = df[df["ID_Zawodnika"].notna()]
    after = len(df)
    print(f"⚠ Usunięto {before - after} wierszy bez ID_Zawodnika")

    df["ID_Regat"] = pd.to_numeric(df["ID_Regat"], errors="coerce").astype("Int64")
    df["WynikWRegatach"] = pd.to_numeric(df["WynikWRegatach"], errors="coerce").astype("Int64")

    df["ID_wystepowania"] = pd.NA

    if "Trening" not in df.columns:
        df["Trening"] = pd.NA

    df["Skrot_norm"] = df["Skrot"].map(_norm_key)
    df["ID_wariantu_klubu"] = df["Skrot_norm"].map(club_map)

    missing_df = df[df["ID_wariantu_klubu"].isna()][
        ["ID_Zawodnika", "ID_Regat", "Skrot"]
    ].drop_duplicates()

    cols = [
        "ID_wystepowania",
        "ID_Zawodnika",
        "ID_Regat",
        "ID_wariantu_klubu",
        "WynikWRegatach",
        "Trening",
    ]
    return df[cols], missing_df


def main():
    df = load_input()
    print(f"▶ Wczytano {len(df)} wierszy wejściowych.")

    club_map = load_club_variant_map(CLUBS_FILE)
    final_df, missing_df = prepare_for_db(df, club_map)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
    print(f"💾 Zapisano: {OUT_FILE} ({len(final_df)} rekordów)")

    if not missing_df.empty:
        missing_df.to_csv(OUT_MISSING, index=False, encoding="utf-8-sig")
        print(f"⚠ Zapisano brakujące kluby do: {OUT_MISSING}")

    print("🎉 Gotowe – plik zgodny z tabelą liga_Wystepowanie_w_regatach.")


if __name__ == "__main__":
    main()
