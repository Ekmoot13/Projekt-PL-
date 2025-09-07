"""
tworzenieWystepowania.py  (z trybem AUTO)
-----------------------------------------
Buduje rekordy do tabeli: liga_Wystepowanie_w_regatach
oraz przygotowuje dane do wstawienia do pod-elementu "Wynik w regatach"
na podstawie plików CSV.

Wariant A (pojedynczy rok + poziom):
    python tworzenieWystepowania.py single --rok 2024 --poziom "Ekstraklasa" --roster ./mnt/data/Zawodnicy_Ekstraklsa_2024.csv

Wariant B (wiele lat / pełny plik):
    python tworzenieWystepowania.py multi --full ./mnt/data/zawodnicy_ekstraklasa_wystepy_full.csv

Wariant C (AUTO: sam znajdź warianty w katalogu ./mnt/data/zawodnicy/** i uruchom A dla każdego):
    python tworzenieWystepowania.py auto
    # opcje:
    #   --base ./mnt/data/zawodnicy   (domyślnie)
    #   --dry-run                     (tylko wypisz, co zostanie zrobione)
    #   --prefer-multi                (jeśli znajdzie plik pełny, użyj trybu 'multi' zamiast wielu 'single')

Wyjścia:
- ./mnt/data/output/wystepowania/<Poziom>_<Rok>_wystepowania.csv
- ./mnt/data/output/wystepowania_multi.csv  (dla wariantu multi)
- ./mnt/data/output/wynikRegat_for_db_*.csv
"""

import os
import re
import sys
import glob
import argparse
import hashlib
import pandas as pd
from pathlib import Path

# --- Ścieżki ---
OUT_DIR = "./mnt/data/output"
WR_OUT_DIR = OUT_DIR  # gdzie zapiszemy wynikRegat_for_db.csv

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)

# --- Pomocnicze: generowanie ID (opcjonalnie, gdy chcesz mieć własne ID_wystepowania w CSV) ---
def numeric_id_from(*parts: str, mod: int = 100_000_000) -> str:
    base = "|".join(str(p) for p in parts)
    h = hashlib.sha1(base.encode("utf-8")).digest()
    num = int.from_bytes(h[:4], "big") % mod
    return f"{num:08d}"

# --- Normalizacja poziomu ligi ---
def _canon_liga(name: str) -> str | None:
    if name is None:
        return None
    s = str(name).strip()
    if not s or s.lower() in ("nan","none"):
        return None
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s)
    low = s.lower()
    if "ekstra" in low or low == "ekstraklasa":
        return "Ekstraklasa"
    m = re.search(r"(^|\b)(\d+)\s*[\.\-]?\s*liga\b", low)
    if m:
        return f"{int(m.group(2))} Liga"
    m2 = re.search(r"^(\d+)\s*liga?$", low.replace(" ", ""))
    if m2:
        return f"{int(m2.group(1))} Liga"
    return None

# --- Szukanie/ładowanie plików ---

def load_all_wynikRegat() -> pd.DataFrame:
    candidates = [
        "./mnt/data/all_wynikRegat.csv",
        "./mnt/data/output/all_wynikRegat.csv"
    ]
    for p in candidates:
        if os.path.isfile(p) and os.path.getsize(p) > 0:
            df = pd.read_csv(p)
            # normalizacja nagłówków
            rename = {}
            low = {str(c).strip().lower(): c for c in df.columns}
            if 'id' in low: rename[low['id']] = 'ID'
            if 'regaty' in low: rename[low['regaty']] = 'regaty'
            if 'klub' in low: rename[low['klub']] = 'klub'
            if 'miejscewregatach' in low: rename[low['miejscewregatach']] = 'miejsceWRegatach'
            if 'miejscowregatach' in low: rename[low['miejscowregatach']] = 'miejsceWRegatach'
            if rename: df = df.rename(columns=rename)
            # typy
            if 'regaty' in df.columns:
                df['regaty'] = pd.to_numeric(df['regaty'], errors='coerce').astype('Int64')
            if 'miejsceWRegatach' in df.columns:
                df['miejsceWRegatach'] = pd.to_numeric(df['miejsceWRegatach'], errors='coerce').astype('Int64')
            return df
    raise FileNotFoundError("Nie znaleziono all_wynikRegat.csv (szukano w ./mnt/data/ oraz ./mnt/data/output).")

def load_all_regaty() -> pd.DataFrame | None:
    candidates = [
        "./mnt/data/output/all_regaty.csv"
    ]
    for p in candidates:
        if os.path.isfile(p) and os.path.getsize(p) > 0:
            df = pd.read_csv(p)
            # normalizacja
            rename = {}
            low = {str(c).strip().lower(): c for c in df.columns}
            if 'id_regat' in low: rename[low['id_regat']] = 'ID_Regat'
            if 'nazwa' in low: rename[low['nazwa']] = 'Nazwa'
            if 'liga_poziom' in low: rename[low['liga_poziom']] = 'Liga_Poziom'
            if 'miasto' in low: rename[low['miasto']] = 'Miasto'
            if 'numer_rundy' in low: rename[low['numer_rundy']] = 'Numer_Rundy'
            if 'rok' in low: rename[low['rok']] = 'Rok'
            if rename: df = df.rename(columns=rename)
            if 'Rok' in df.columns:
                df['Rok'] = pd.to_numeric(df['Rok'], errors='coerce').astype('Int64')
            return df
    return None

def _rename_for_roster(df: pd.DataFrame) -> pd.DataFrame:
    low = {str(c).strip().lower(): c for c in df.columns}
    rename = {}
    if 'id_zawodnika' in low: rename[low['id_zawodnika']] = 'ID_Zawodnika'
    if 'id' in low and 'ID_Zawodnika' not in rename.values(): rename[low['id']] = 'ID_Zawodnika'
    if 'skrót' in low: rename[low['skrót']] = 'Skrot'
    if 'skrot' in low: rename[low['skrot']] = 'Skrot'
    if 'klub' in low and 'Skrot' not in rename.values(): rename[low['klub']] = 'Skrot'
    if 'rok' in low: rename[low['rok']] = 'Rok'
    if 'liga_poziom' in low: rename[low['liga_poziom']] = 'Liga_Poziom'
    if rename: df = df.rename(columns=rename)
    return df

def load_roster(paths: list[str]) -> pd.DataFrame:
    frames = []
    for p in paths:
        if not os.path.isfile(p) or os.path.getsize(p) == 0:
            print(f"⚠ Pomijam roster (brak/ pusty): {p}")
            continue
        df = pd.read_csv(p)
        df = _rename_for_roster(df)

        missing = [c for c in ['ID_Zawodnika','Skrot'] if c not in df.columns]
        if missing:
            print(f"⏭️  Pomijam {p} – brak kolumn: {missing}")
            continue
        frames.append(df[['ID_Zawodnika','Skrot'] + ([c for c in ['Rok','Liga_Poziom'] if c in df.columns])])

    if not frames:
        raise ValueError("Nie udało się wczytać żadnego rosteru z wymaganymi kolumnami (ID_Zawodnika, Skrot).")
    out = pd.concat(frames, ignore_index=True).drop_duplicates()
    # sanity
    out['ID_Zawodnika'] = pd.to_numeric(out['ID_Zawodnika'], errors='coerce').astype('Int64')
    out = out.dropna(subset=['ID_Zawodnika','Skrot'])
    out['Skrot'] = out['Skrot'].astype(str).str.strip()
    return out

# --- Logika łączenia ---

def build_wystepowania_for_year_level(rok: int, poziom: str, roster_paths: list[str]):
    wr = load_all_wynikRegat()
    regaty = load_all_regaty()

    # Filtrowanie regat po roku i poziomie
    if regaty is None:
        wr_f = wr.copy()
        print("⚠ Brak all_regaty.csv – nie filtruję po roku/poziomie (użyję wszystkich regat z all_wynikRegat.csv).")
    else:
        wr_f = wr.merge(regaty[['ID_Regat','Rok','Liga_Poziom']], left_on='regaty', right_on='ID_Regat', how='left')
        wr_f = wr_f[(wr_f['Rok'] == int(rok)) & (wr_f['Liga_Poziom'].astype(str).str.lower() == str(poziom).lower())]
        if wr_f.empty:
            print(f"⚠ Brak wyników dla {poziom} {rok} w all_wynikRegat.csv + all_regaty.csv")
            return None

    roster = load_roster(roster_paths)
    kluby_set = set(wr_f['klub'].astype(str).str.strip().unique())
    roster_f = roster[roster['Skrot'].astype(str).str.strip().isin(kluby_set)].copy()
    if roster_f.empty:
        print("⚠ Lista zawodników po filtrze klubów jest pusta.")
        return None

    join_df = wr_f[['regaty','klub']].drop_duplicates().merge(
        roster_f[['ID_Zawodnika','Skrot']], left_on='klub', right_on='Skrot', how='left'
    ).dropna(subset=['ID_Zawodnika'])

    wyst = pd.DataFrame({
        "ID_wystepowania": [
            numeric_id_from("wyst", r, int(z), k)
            for r, z, k in zip(join_df['regaty'], join_df['ID_Zawodnika'], join_df['klub'])
        ],
        "ID_Zawodnika": join_df['ID_Zawodnika'].astype('Int64'),
        "ID_Regat": join_df['regaty'].astype('Int64'),
        "Skrot": join_df['klub'].astype(str).str.strip(),
        "Trening": ["NIE"] * len(join_df)
    })

    ensure_dirs()
    out_path = os.path.join(OUT_DIR, f"wystepowania/{poziom.replace(' ','')}_{rok}_wystepowania.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    wyst.to_csv(out_path, index=False)
    print(f"✅ Zapisano występowania: {out_path} ({len(wyst)} rekordów)")

    wr_subset = wr_f[['regaty','klub','miejsceWRegatach']].drop_duplicates()
    wr_subset.insert(0, 'ID', [numeric_id_from("wynikRegat", r, k) for r, k in zip(wr_subset['regaty'], wr_subset['klub'])])
    wr_out = os.path.join(WR_OUT_DIR, f"wynikRegat_for_db_{poziom.replace(' ','')}_{rok}.csv")
    wr_subset.to_csv(wr_out, index=False)
    print(f"✅ Zapisano wynikRegat_for_db: {wr_out} ({len(wr_subset)} rekordów)")

    return {"wystepowania": out_path, "wynikRegat": wr_out}

def build_wystepowania_multi(full_path: str):
    wr = load_all_wynikRegat()
    regaty = load_all_regaty()
    roster = load_roster([full_path])

    if regaty is None:
        wr_meta = wr.copy()
        wr_meta['Rok'] = pd.NA
        wr_meta['Liga_Poziom'] = pd.NA
    else:
        wr_meta = wr.merge(regaty[['ID_Regat','Rok','Liga_Poziom']], left_on='regaty', right_on='ID_Regat', how='left')

    roster['Skrot'] = roster['Skrot'].astype(str).str.strip()
    wr_meta['klub'] = wr_meta['klub'].astype(str).str.strip()

    join_df = wr_meta[['regaty','klub','Rok','Liga_Poziom']].drop_duplicates().merge(
        roster[['ID_Zawodnika','Skrot']], left_on='klub', right_on='Skrot', how='left'
    ).dropna(subset=['ID_Zawodnika'])

    wyst = pd.DataFrame({
        "ID_wystepowania": [
            numeric_id_from("wyst", r, int(z), k)
            for r, z, k in zip(join_df['regaty'], join_df['ID_Zawodnika'], join_df['klub'])
        ],
        "ID_Zawodnika": join_df['ID_Zawodnika'].astype('Int64'),
        "ID_Regat": join_df['regaty'].astype('Int64'),
        "Skrot": join_df['klub'].astype(str).str.strip(),
        "Trening": ["NIE"] * len(join_df)
    })

    ensure_dirs()
    out_path = os.path.join(OUT_DIR, "wystepowania_multi.csv")
    wyst.to_csv(out_path, index=False)
    print(f"✅ Zapisano występowania (multi): {out_path} ({len(wyst)} rekordów)")

    wr_copy = wr[['regaty','klub','miejsceWRegatach']].drop_duplicates()
    wr_copy.insert(0, 'ID', [numeric_id_from("wynikRegat", r, k) for r, k in zip(wr_copy['regaty'], wr_copy['klub'])])
    wr_out = os.path.join(WR_OUT_DIR, "wynikRegat_for_db_all.csv")
    wr_copy.to_csv(wr_out, index=False)
    print(f"✅ Zapisano wynikRegat_for_db (all): {wr_out} ({len(wr_copy)} rekordów)")

    return {"wystepowania": out_path, "wynikRegat": wr_out}

# --- AUTO: skan katalogu zawodnicy i uruchom "single" dla znalezionych wariantów ---

def _extract_year_from_path(p: str) -> int | None:
    for seg in Path(p).parts:
        if re.fullmatch(r"(19|20)\d{2}", str(seg)):
            return int(seg)
    # spróbuj nazwy pliku
    m = re.search(r"(19|20)(\d{2})", Path(p).name)
    return int(m.group()) if m else None

def _extract_level_from_path(p: str) -> str | None:
    # sprawdź segmenty ścieżki i samą nazwę pliku
    for seg in list(Path(p).parts) + [Path(p).stem]:
        can = _canon_liga(seg)
        if can:
            return can
    return None

def detect_variants(base: str = "./mnt/data/zawodnicy"):
    """
    Szuka plików z rosterem w base/**, które zawierają przynajmniej kolumny ID_Zawodnika i Skrot
    (z elastycznym mapowaniem nazw). Grupuje je w (rok, poziom) -> [ścieżki].
    Dodatkowo wykrywa potencjalny 'full roster' (z kolumną Rok i wieloma latami).
    """
    per_variant = {}  # (rok, poziom) -> list[path]
    full_candidates = []

    for csv in glob.glob(os.path.join(base, "**", "*.csv"), recursive=True):
        try:
            df = pd.read_csv(csv, nrows=50)  # szybkie sprawdzenie nagłówków
        except Exception:
            continue
        df = _rename_for_roster(df)

        if not set(["ID_Zawodnika","Skrot"]) <= set(df.columns):
            continue

        rok = _extract_year_from_path(csv)
        poziom = _extract_level_from_path(csv)

        if "Rok" in df.columns:
            # jeśli zawiera kolumnę Rok – kandydat na full
            full_candidates.append(csv)

        if rok is not None and poziom is not None:
            per_variant.setdefault((rok, poziom), []).append(csv)

    # heurystyka wyboru jednego full: najwięcej wierszy
    full_path = None
    if full_candidates:
        try:
            sizes = []
            for p in full_candidates:
                try:
                    cnt = sum(1 for _ in open(p, "rb"))
                except Exception:
                    cnt = 0
                sizes.append((cnt, p))
            sizes.sort(reverse=True)
            full_path = sizes[0][1]
        except Exception:
            full_path = full_candidates[0]

    return per_variant, full_path

def run_auto(base: str = "./mnt/data/zawodnicy", dry_run: bool = False, prefer_multi: bool = False):
    per_variant, full_path = detect_variants(base)
    if not per_variant and not full_path:
        print(f"⚠ Nie znaleziono żadnych rosterów w {base}.")
        return

    print("🔎 Wykryte warianty (rok, poziom) -> pliki:")
    for (rok, poziom), paths in sorted(per_variant.items()):
        print(f"  - ({rok}, {poziom}): {len(paths)} plików")
        for p in paths:
            print(f"      • {p}")

    if full_path:
        print(f"🔎 Wykryty kandydat FULL: {full_path}")

    if dry_run:
        print("🧪 DRY RUN – nic nie uruchamiam.")
        return

    if prefer_multi and full_path:
        print("▶ Uruchamiam tryb MULTI na wykrytym pliku pełnym.")
        build_wystepowania_multi(full_path)
        return

    # domyślnie: odpal 'single' dla każdego (rok, poziom)
    for (rok, poziom), paths in sorted(per_variant.items()):
        print(f"▶ Uruchamiam SINGLE dla: {poziom} {rok}")
        build_wystepowania_for_year_level(int(rok), str(poziom), paths)

# --- CLI ---
def main():
    parser = argparse.ArgumentParser(description="Tworzenie występowania w regatach + wynikRegat import.")
    sub = parser.add_subparsers(dest="mode", required=True)

    p1 = sub.add_parser("single", help="Pojedynczy rok + poziom")
    p1.add_argument("--rok", type=int, required=True)
    p1.add_argument("--poziom", type=str, required=True, help='Np. "Ekstraklasa", "1 Liga"')
    p1.add_argument("--roster", nargs="+", required=True, help="Plik(i) CSV z zawodnikami dla danego roku/poziomu")

    p2 = sub.add_parser("multi", help="Wiele lat (pełny plik)")
    p2.add_argument("--full", type=str, required=True, help="CSV z zawodnikami z wielu lat (z kolumnami ID_Zawodnika, Skrot)")

    p3 = sub.add_parser("auto", help="Skanuj ./mnt/data/zawodnicy i uruchom odpowiednie warianty")
    p3.add_argument("--base", type=str, default="./mnt/data/zawodnicy")
    p3.add_argument("--dry-run", action="store_true")
    p3.add_argument("--prefer-multi", action="store_true")

    args = parser.parse_args()

    if args.mode == "single":
        build_wystepowania_for_year_level(args.rok, args.poziom, args.roster)
    elif args.mode == "multi":
        build_wystepowania_multi(args.full)
    else:
        run_auto(base=args.base, dry_run=args.dry_run, prefer_multi=args.prefer_multi)

if __name__ == "__main__":
    main()
