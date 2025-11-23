# -*- coding: utf-8 -*-
"""
tworzenie_wystepowania_z_xlsx.py / export_wystepowania_from_PLZ2025.py
WERSJA: ID_Regat brane z plików _regaty.csv wygenerowanych przez main.py
"""

import pandas as pd
import hashlib
import re
import unicodedata
from pathlib import Path
import os

# Wejścia / wyjścia
SRC_PLZ2025 = Path("mnt/data/występowanie/PLZ_uczestnicy_2025.xlsx")
OUT_DIR = Path("mnt/data/output/wystepowanie/xlsx")
CLUBS_FILE = Path("mnt/data/kluby/kluby_wyciag.csv")
MAIN_OUTPUT_REGATY_DIR = Path("mnt/data/output/main")   # tu main.py zapisuje *_regaty.csv


# -------------------------------
# Helpery
# -------------------------------

def strip_accents_lower(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()


def _norm_key(s: str) -> str:
    """Normalizacja skrótu klubu – tak samo jak w main.py / kluby.py."""
    if s is None:
        return ""
    base = str(s).strip()
    base = re.sub(r"\s+", " ", base)
    base = base.replace(" ", "")
    return base.upper()


def load_club_variant_map(path: Path) -> dict:
    """
    Mapa: znormalizowany Skrot -> ID_wariantu_klubu
    z pliku kluby_wyciag.csv (z kluby.py).
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


def generate_player_id_only_name(fullname: str) -> int:
    norm = strip_accents_lower(fullname)
    base = f"zawodnik|name_norm={norm}"
    h = hashlib.sha1(base.encode()).digest()
    return int.from_bytes(h[:4], "big") % 100_000_000


def extract_tag(klub_cell: str) -> str:
    """
    Wyciąga skrót klubu z komórki 'Klub', np. 'Sport Vita Ski&Sail (SPO)' -> 'SPO'.
    Jeśli nie ma nawiasu, próbuje zgadnąć ostatni wielki token.
    """
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
    """
    Mapuje opis z kolumny 'Regaty' na poziom ligi, starając się
    użyć takiego samego stringa jak w kolumnie Liga_Poziom w plikach _regaty.csv.
    W razie rozjazdu zobaczysz to w pliku brakujace_regaty.csv.
    """
    t = str(cell or "").strip().lower()
    if "młodzież" in t or "mlodziez" in t:
        return "Youth"
    if "regional" in t:
        return "Ligi Regionalne – Finał"
    if "ekstra" in t:
        return "Ekstraklasa"
    if ("1" in t and "liga" in t) or "i liga" in t:
        return "1 Liga"
    # fallback – możesz tu dopasować do swoich nazw folderów
    return "Ekstraklasa"


# -------------------------------
# Mapa Regat z plików main.py
# -------------------------------

def load_regaty_map(regaty_dir: Path) -> pd.DataFrame:
    """
    Wczytuje wszystkie pliki *_regaty.csv z outputu main.py
    i zwraca DataFrame z kolumnami:
      ID_Regat, Liga_Poziom, Numer_Rundy, Rok
    """
    rows = []
    if not regaty_dir.exists():
        print(f"⚠ Katalog z regatami nie istnieje: {regaty_dir}")
        return pd.DataFrame(columns=["ID_Regat", "Liga_Poziom", "Numer_Rundy", "Rok"])

    for fname in os.listdir(regaty_dir):
        if not fname.endswith("_regaty.csv"):
            continue
        path = regaty_dir / fname
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"⏭️ Pomijam {path}: {e}")
            continue

        required = {"ID_Regat", "Liga_Poziom", "Numer_Rundy", "Rok"}
        if not required.issubset(df.columns):
            print(f"⚠ Plik {path} nie ma wymaganych kolumn {required}, pomijam.")
            continue

        rows.append(df[["ID_Regat", "Liga_Poziom", "Numer_Rundy", "Rok"]])

    if not rows:
        print("⚠ Nie znaleziono żadnych plików *_regaty.csv.")
        return pd.DataFrame(columns=["ID_Regat", "Liga_Poziom", "Numer_Rundy", "Rok"])

    all_reg = pd.concat(rows, ignore_index=True)
    # usuwamy duplikaty (gdyby coś się powtórzyło)
    all_reg = all_reg.drop_duplicates(subset=["ID_Regat", "Liga_Poziom", "Numer_Rundy", "Rok"])
    print(f"📚 Załadowano regaty: {len(all_reg)} rekordów z plików _regaty.csv")
    return all_reg


# -------------------------------
# GŁÓWNY SKRYPT
# -------------------------------

def main():
    print("🏁 Generuję występowania z PLZ_uczestnicy_2025.xlsx (ID_Regat z _regaty.csv)...")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    club_map = load_club_variant_map(CLUBS_FILE)
    regaty_df = load_regaty_map(MAIN_OUTPUT_REGATY_DIR)

    if regaty_df.empty:
        print("❌ Brak danych regat (pliki *_regaty.csv). Przerwij i odpal najpierw main.py.")
        return

    missing_codes = set()

    ucz = pd.read_excel(SRC_PLZ2025)
    ucz.columns = [c.strip().lower() for c in ucz.columns]

    imie_col = "imię" if "imię" in ucz.columns else "imie"
    nazw_col = "nazwisko"
    runda_col = "runda"
    klub_col = "klub"
    regaty_col = "regaty"

    # budujemy DataFrame wystąpień wejściowych
    wyst = pd.DataFrame({
        "Zawodnik": (ucz[imie_col].astype(str).str.strip() + " " +
                     ucz[nazw_col].astype(str).str.strip()).str.replace(r"\s+", " ", regex=True),
        "Skrot": ucz[klub_col].apply(extract_tag),
        "rok": 2025,
        "regaty": ucz[regaty_col].astype(str),
        "numer_rundy": pd.to_numeric(ucz[runda_col], errors="coerce").astype("Int64"),
        "poziom_ligi": ucz[regaty_col].apply(liga_from_regaty_cell),
    }).dropna(subset=["Zawodnik", "Skrot", "numer_rundy"])

    # dopasowujemy ID_Regat po (Rok, Numer_Rundy, Liga_Poziom)
    merge_left = wyst.copy()
    merge_left["Rok"] = merge_left["rok"].astype(int)
    merge_left["Numer_Rundy"] = merge_left["numer_rundy"].astype(int)
    merge_left["Liga_Poziom"] = merge_left["poziom_ligi"]

    merged = merge_left.merge(
        regaty_df,
        on=["Rok", "Numer_Rundy", "Liga_Poziom"],
        how="left",
        suffixes=("", "_reg")
    )

    # jeśli nie znaleziono ID_Regat – logujemy to do osobnego pliku
    missing_reg = merged[merged["ID_Regat"].isna()][
        ["Zawodnik", "Skrot", "rok", "numer_rundy", "poziom_ligi", "regaty"]
    ]

    if not missing_reg.empty:
        miss_path = OUT_DIR / "wystepowanie_all_brak_regat.csv"
        missing_reg.to_csv(miss_path, index=False, encoding="utf-8-sig")
        print(f"⚠ {len(missing_reg)} wierszy nie dopasowało ID_Regat – zapisano do: {miss_path}")
        print("   Sprawdź, czy 'poziom_ligi' i 'numer_rundy' zgadzają się z danymi w _regaty.csv")

    # rekordy z poprawnym ID_Regat
    ok = merged[merged["ID_Regat"].notna()].copy()

    # liczymy ID_Zawodnika i ID_wariantu_klubu
    rows = []
    for _, r in ok.iterrows():
        full_name = str(r["Zawodnik"]).strip()
        skrot_klubu = str(r["Skrot"]).strip()

        id_zaw = generate_player_id_only_name(full_name)
        id_regat = int(r["ID_Regat"])

        id_wariantu = club_map.get(_norm_key(skrot_klubu))
        if id_wariantu is None:
            missing_codes.add(skrot_klubu)

        rows.append({
            "ID_wystepowania": "",
            "ID_Zawodnika": id_zaw,
            "ID_Regat": id_regat,
            "ID_wariantu_klubu": id_wariantu,
            "WynikWRegatach": "",
            "Trening": "",
        })

    if not rows:
        print("⚠ Po odfiltrowaniu braków nie ma wierszy do zapisania.")
        return

    out_df = pd.DataFrame(rows)
    out_path = OUT_DIR / "wystepowanie_all.csv"
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"✅ Zapisano: {out_path} ({len(out_df)} rekordów)")
    print(out_df.head(15).to_string(index=False))

    if missing_codes:
        miss_codes_path = OUT_DIR / "wystepowanie_all_brak_wariantu_klubu.csv"
        pd.DataFrame(sorted(missing_codes), columns=["Skrot"]).to_csv(
            miss_codes_path, index=False, encoding="utf-8-sig"
        )
        print(f"⚠ Brak ID_wariantu_klubu dla skrótów: {sorted(missing_codes)}")
        print(f"   Zapisano do: {miss_codes_path}")

    print("🎉 Gotowe – ID_Regat są zgodne z danymi z main.py.")


if __name__ == "__main__":
    main()
