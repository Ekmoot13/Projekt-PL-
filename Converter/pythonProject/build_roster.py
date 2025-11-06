
import os
import re
import glob
import pandas as pd
from pathlib import Path

BASE_DIRS = ["./mnt/data/zawodnicy", "./mnt/data"]  # skanuj oba miejsca
OUT_DIR = "mnt/data/output/roster"
os.makedirs(OUT_DIR, exist_ok=True)

# --------- helpers ---------
def _norm_space(s: str) -> str:
    s = str(s) if s is not None else ""
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _col_rename(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    low = {str(c).strip().lower(): c for c in df.columns}
    rename = {}
    for want, alts in mapping.items():
        for a in alts:
            key = str(a).strip().lower()
            if key in low:
                rename[low[key]] = want
                break
    if rename:
        df = df.rename(columns=rename)
    return df

def read_csv_safe(path, nrows=None):
    try:
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            return None
        return pd.read_csv(path, nrows=nrows)
    except Exception:
        return None

# --------- scan roster sources (name+club) ---------
def scan_roster_sources():
    rows = []
    files = set()
    for base in BASE_DIRS:
        if not os.path.isdir(base):
            continue
        files.update(glob.glob(os.path.join(base, "**", "*.csv"), recursive=True))
    for f in sorted(files):
        df = read_csv_safe(f)
        if df is None:
            continue
        # mapuj potencjalne kolumny
        df = _col_rename(df, {
            "Zawodnik": ["Zawodnik","Imie i nazwisko","Imię i nazwisko","Nazwisko i imię"],
            "Skrot": ["Skrót","Skrot","Klub","Zespół","Zespol","Skrót klubu","Skrot klubu"]
        })
        if not set(["Zawodnik","Skrot"]).issubset(df.columns):
            continue
        tmp = df[["Zawodnik","Skrot"]].copy()
        tmp["Zawodnik"] = tmp["Zawodnik"].astype(str).map(_norm_space)
        tmp["Skrot"] = tmp["Skrot"].astype(str).map(_norm_space)
        tmp["__src__"] = f
        tmp = tmp[(tmp["Zawodnik"]!="") & (tmp["Skrot"]!="")]
        if not tmp.empty:
            rows.append(tmp)
    if not rows:
        return pd.DataFrame(columns=["Zawodnik","Skrot","__src__"])
    out = pd.concat(rows, ignore_index=True).drop_duplicates()
    return out

# --------- scan mapping sources (name→ID) ---------
def scan_mapping_sources():
    """
    Szuka plików z kolumnami zawierającymi ID i nazwę zawodnika.
    """
    rows = []
    files = set()
    pat = re.compile(r"(zawodnic|zgloszen|zgłoszen|master|lista|id)", re.IGNORECASE)
    for base in BASE_DIRS:
        if not os.path.isdir(base):
            continue
        for f in glob.glob(os.path.join(base, "**", "*.csv"), recursive=True):
            if pat.search(Path(f).name):
                files.add(f)
    # dorzuć znane nazwy
    for f in ["./mnt/data/Zawodnict_zgłoszenia.csv"]:
        if os.path.isfile(f):
            files.add(f)

    for f in sorted(files):
        df = read_csv_safe(f)
        if df is None:
            continue
        df = _col_rename(df, {
            "Zawodnik": ["Zawodnik","Imie i nazwisko","Imię i nazwisko","Nazwisko i imię","Nazwisko i Imię"],
            "ID_Zawodnika": ["ID_Zawodnika","ID","Id","id zawodnika","Id_zawodnika"]
        })
        if not set(["Zawodnik","ID_Zawodnika"]).issubset(df.columns):
            continue
        tmp = df[["Zawodnik","ID_Zawodnika"]].copy()
        tmp["Zawodnik"] = tmp["Zawodnik"].astype(str).map(_norm_space)
        # spróbuj rzutować ID na int – ale zachowaj jako string (niektórzy mają wiodące zera)
        tmp["ID_Zawodnika"] = pd.to_numeric(tmp["ID_Zawodnika"], errors="coerce").astype("Int64").astype(str)
        tmp["__src__"] = f
        tmp = tmp[tmp["Zawodnik"]!=""]
        if not tmp.empty:
            rows.append(tmp)
    if not rows:
        return pd.DataFrame(columns=["Zawodnik","ID_Zawodnika","__src__"])
    out = pd.concat(rows, ignore_index=True).drop_duplicates(subset=["Zawodnik","ID_Zawodnika"])
    return out

# --------- build roster ---------
def build_and_save():
    roster_src = scan_roster_sources()
    if roster_src.empty:
        print("⚠ Nie znaleziono żadnych plików z kolumnami (Zawodnik, Klub/Skrót).")
        return
    mapping = scan_mapping_sources()

    # join po nazwisku
    if mapping.empty:
        print("⚠ Nie znaleziono mapy Zawodnik→ID_Zawodnika. Zapisuję tylko listę do uzupełnienia.")
        resolved = pd.DataFrame(columns=["ID_Zawodnika","Skrot","Zawodnik"])
        unresolved = roster_src[["Zawodnik","Skrot"]].drop_duplicates()
    else:
        j = roster_src.merge(mapping[["Zawodnik","ID_Zawodnika"]], on="Zawodnik", how="left")
        resolved = j.dropna(subset=["ID_Zawodnika"])[["ID_Zawodnika","Skrot","Zawodnik"]].drop_duplicates()
        unresolved = j[j["ID_Zawodnika"].isna()][["Zawodnik","Skrot"]].drop_duplicates()

    # roster.csv = tylko resolved (bez kolumny Zawodnik, bo do bazy wystarczą ID+Skrot)
    roster = resolved[["ID_Zawodnika","Skrot"]].drop_duplicates()
    roster_path = os.path.join(OUT_DIR, "roster.csv")
    roster.to_csv(roster_path, index=False)

    # plik pomocniczy do ręcznego uzupełnienia braków
    missing_path = os.path.join(OUT_DIR, "roster_missing_ids.csv")
    unresolved.to_csv(missing_path, index=False)

    # raport szczegółowy (z nazwiskami w resolved)
    resolved_path = os.path.join(OUT_DIR, "roster_resolved_with_names.csv")
    resolved.to_csv(resolved_path, index=False)

    print(f"✅ roster.csv: {roster_path} ({len(roster)} wierszy)")
    print(f"✅ roster_missing_ids.csv: {missing_path} ({len(unresolved)} braków)")
    print(f"✅ roster_resolved_with_names.csv: {resolved_path} ({len(resolved)} wierszy)")

if __name__ == "__main__":
    build_and_save()
