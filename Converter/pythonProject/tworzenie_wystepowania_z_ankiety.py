# -*- coding: utf-8 -*-
"""
tworzenie_wystepowania_z_ankiety.py – PROSTA WERSJA (FILTROWANIE + KOL. ID_wystepowania)

✔ Nie łączy z zawodnikami ani regatami.
✔ Pomija wiersze bez ID_Zawodnika.
✔ Dodaje kolumnę ID_wystepowania (pustą).
✔ Zwraca jeden plik: wystepowania_z_ankiety_all.csv.
"""

from pathlib import Path
import pandas as pd


BASE_DIR = Path("mnt/data")

IN_FILE_CANDIDATES = [
    BASE_DIR / "występowanie" / "wystepowania_z_ankiety_completed.csv",
    BASE_DIR / "wystepowania_z_ankiety_completed.csv",
]

OUT_DIR = BASE_DIR / "output" / "wystepowanie" / "ankieta"
OUT_FILE = OUT_DIR / "wystepowania_z_ankiety_all.csv"


def load_input() -> pd.DataFrame:
    for path in IN_FILE_CANDIDATES:
        if path.exists():
            print(f"📥 Czytam wejście z: {path}")
            return pd.read_csv(path, encoding="utf-8")
    raise FileNotFoundError(
        "Brak pliku 'wystepowania_z_ankiety.csv' ani w 'mnt/data', "
        "ani w 'mnt/data/występowanie'."
    )


def prepare_for_db(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required = ["ID_Zawodnika", "ID_Regat", "Skrot", "WynikWRegatach"]

    for col in required:
        if col not in df.columns:
            raise ValueError(f"Brakuje obowiązkowej kolumny: {col}")

    # Upewniamy się, że ID to liczby
    df["ID_Zawodnika"] = pd.to_numeric(df["ID_Zawodnika"], errors="coerce").astype("Int64")

    # ❗ USUWAMY wiersze bez ID_Zawodnika
    before = len(df)
    df = df[df["ID_Zawodnika"].notna()]
    after = len(df)

    print(f"⚠ Usunięto {before - after} wierszy bez ID_Zawodnika")

    # Konwersje typów
    df["ID_Regat"] = pd.to_numeric(df["ID_Regat"], errors="coerce").astype("Int64")
    df["WynikWRegatach"] = pd.to_numeric(df["WynikWRegatach"], errors="coerce").astype("Int64")

    # Dodajemy kolumnę ID_wystepowania (pustą)
    df["ID_wystepowania"] = pd.NA

    # Dodaj Trening jeśli nie istnieje
    if "Trening" not in df.columns:
        df["Trening"] = pd.NA

    # Kolejność zgodna z tabelą MySQL: liga_Wystepowanie_w_regatach
    cols = [
        "ID_wystepowania",
        "ID_Zawodnika",
        "ID_Regat",
        "Skrot",
        "WynikWRegatach",
        "Trening"
    ]

    return df[cols]


def main():
    df = load_input()
    print(f"▶ Wczytano {len(df)} wierszy wejściowych.")

    final_df = prepare_for_db(df)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

    print(f"💾 Zapisano: {OUT_FILE} ({len(final_df)} rekordów)")
    print("🎉 Gotowe – plik w pełni zgodny z tabelą liga_Wystepowanie_w_regatach.")


if __name__ == "__main__":
    main()
