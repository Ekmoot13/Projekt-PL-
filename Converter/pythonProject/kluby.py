
import os
import re
import glob
import pandas as pd
from collections import defaultdict, Counter
from pathlib import Path

BASE_DIR = "./mnt/data/Regaty"
OUT_DIR  = "./mnt/data/kluby"
os.makedirs(OUT_DIR, exist_ok=True)

def _norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    # ujednolicenie białych znaków
    s = re.sub(r"\s+", " ", s)
    return s

def _norm_key(s: str) -> str:
    """Klucz porównawczy: uppercase + bez zbędnych spacji."""
    return _norm(s).upper()

def find_col(columns, patterns):
    cols = list(columns)
    # pierwsze podejście: dokładne
    for p in patterns:
        for c in cols:
            if c == p:
                return c
    # drugie: case-insensitive + normalizacja diakrytyk
    def simp(x):
        x = str(x).lower().strip()
        # uprość diakrytyki kluczowych liter
        x = x.replace("ó","o").replace("ł","l").replace("ą","a").replace("ę","e").replace("ś","s").replace("ż","z").replace("ź","z").replace("ć","c").replace("ń","n")
        x = re.sub(r"\s+", " ", x)
        return x
    simp_cols = {simp(c): c for c in cols}
    for p in patterns:
        sp = simp(p)
        if sp in simp_cols:
            return simp_cols[sp]
    # trzecie: zawiera sub-słowo (np. "Skrot klubu")
    for c in cols:
        sc = simp(c)
        for p in patterns:
            sp = simp(p)
            if sp in sc:
                return c
    return None

def scan_clubs():
    """
    Przechodzi po wszystkich plikach CSV w BASE_DIR i zbiera pary (Skrót, Nazwa).
    Obsługuje różne nagłówki, np. 'Skrót'/'Skrot' i 'Klub'/'Nazwa'.
    """
    raw_rows = []
    seen_files = 0

    for path in glob.glob(os.path.join(BASE_DIR, "**", "*.csv"), recursive=True):
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"⏭️  Pomijam (nieczytelny/pusty): {path} – {e}")
            continue

        seen_files += 1
        # typowe nazwy kolumn
        col_skrot = find_col(df.columns, ["Skrót", "Skrot", "Skrót klubu", "Skrot klubu", "Zespół", "Zespol"])
        col_nazwa = find_col(df.columns, ["Klub", "Nazwa", "Nazwa klubu"])

        if col_skrot is None and col_nazwa is None:
            # nic do zebrania z tego arkusza
            continue

        # spróbujmy odczytać wszystkie wiersze (ignorując NaN)
        for _, r in df.iterrows():
            skrot = _norm(r[col_skrot]) if col_skrot in df.columns else ""
            nazwa = _norm(r[col_nazwa]) if col_nazwa in df.columns else ""

            if not skrot and not nazwa:
                continue

            # jeśli brak skrótu a jest nazwa, spróbuj zrobić skrót roboczy (pierwsze 4 litery, wielkie)
            if not skrot and nazwa:
                # tymczasowy skrót „auto”; użytkownik może to ręcznie później poprawić
                auto = re.sub(r"[^A-Za-z0-9]", "", nazwa.upper())[:6]
                skrot = auto or "TMP"

            raw_rows.append({
                "Plik": path,
                "Skrót": skrot,
                "Nazwa": nazwa
            })

    raw_df = pd.DataFrame(raw_rows)
    return raw_df, seen_files

def build_unique(raw_df: pd.DataFrame):
    """
    Tworzy listę unikalnych klubów wg Skrótu, wybierając najczęściej występującą nazwę.
    Zwraca: unique_df, conflicts_df
    """
    if raw_df.empty:
        return pd.DataFrame(columns=["Skrot","Nazwa","Wystapienia"]), pd.DataFrame(columns=["Skrot","Kandydat_Nazwa","Liczba"])

    # agregacja: per skrót – zlicz nazwy
    by_skrot = defaultdict(Counter)
    counts = Counter()

    for _, r in raw_df.iterrows():
        sk = _norm_key(r["Skrót"])
        nm = _norm(r["Nazwa"])
        counts[sk] += 1
        if nm:
            by_skrot[sk][nm] += 1
        else:
            by_skrot[sk][""] += 1  # pusta nazwa

    unique_rows = []
    conflict_rows = []

    for sk_norm, name_counter in by_skrot.items():
        total = counts[sk_norm]

        # usuń puste nazwy z rankingu, ale zachowaj info o konflikcie gdy były tylko puste
        name_counter_no_empty = Counter({k:v for k,v in name_counter.items() if k.strip()})

        if name_counter_no_empty:
            # wybierz najczęstszą nazwę; przy remisie wybierz dłuższą
            max_count = max(name_counter_no_empty.values())
            candidates = [n for n,c in name_counter_no_empty.items() if c == max_count]
            best = max(candidates, key=len)
        else:
            best = ""

        # oryginalna forma skrótu – weź najczęściej spotkaną wersję zapisu
        forms = [r["Skrót"] for _, r in raw_df.iterrows() if _norm_key(r["Skrót"]) == sk_norm]
        # wybierz najdłuższą spośród najczęstszych form (żeby zachować np. myślniki/kropki)
        form_counts = Counter(forms)
        most_common_freq = form_counts.most_common(1)[0][1] if form_counts else 1
        best_forms = [f for f,c in form_counts.items() if c == most_common_freq]
        skrot_final = max(best_forms, key=len) if best_forms else sk_norm

        unique_rows.append({
            "Skrot": skrot_final,
            "Nazwa": best,
            "Wystapienia": int(total)
        })

        # jeśli istnieje więcej niż jedna sensowna nazwa – zapisz konflikt do raportu
        if len([n for n in name_counter_no_empty.keys()]) > 1:
            for nm, c in name_counter_no_empty.most_common():
                conflict_rows.append({
                    "Skrot": skrot_final,
                    "Kandydat_Nazwa": nm,
                    "Liczba": int(c)
                })

    unique_df = pd.DataFrame(unique_rows).sort_values(["Skrot"]).reset_index(drop=True)
    conflicts_df = pd.DataFrame(conflict_rows).sort_values(["Skrot","Liczba"], ascending=[True,False]).reset_index(drop=True)
    return unique_df, conflicts_df

def save_outputs(raw_df, unique_df, conflicts_df):
    raw_path = os.path.join(OUT_DIR, "kluby_raw.csv")
    unique_path = os.path.join(OUT_DIR, "kluby_unique.csv")
    conflicts_path = os.path.join(OUT_DIR, "kluby_conflicts.csv")
    sql_path = os.path.join(OUT_DIR, "kluby_insert.sql")

    raw_df.to_csv(raw_path, index=False)
    unique_df.to_csv(unique_path, index=False)
    conflicts_df.to_csv(conflicts_path, index=False)

    # SQL pod MySQL: INSERT z ON DUPLICATE KEY UPDATE (Skrot to PK w tabeli liga_Kluby)
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write("-- INSERTY do tabeli liga_Kluby (Skrot, Nazwa)\n")
        f.write("-- Uruchom w phpMyAdmin | MySQL 5.7+/8.0+\n\n")
        for _, r in unique_df.iterrows():
            sk = r["Skrot"].replace("'", "''")
            nm = (r["Nazwa"] or "").replace("'", "''")
            f.write(
                f"INSERT INTO liga_Kluby (Skrot, Nazwa) VALUES ('{sk}', '{nm}') "
                f"ON DUPLICATE KEY UPDATE Nazwa=VALUES(Nazwa);\n"
            )

    print(f"✅ Zapisano: {raw_path}")
    print(f"✅ Zapisano: {unique_path}")
    print(f"✅ Zapisano: {conflicts_path}")
    print(f"✅ Zapisano: {sql_path}")

def main():
    raw_df, seen_files = scan_clubs()
    if raw_df.empty:
        print("⚠ Nie znaleziono żadnych danych klubów w plikach CSV.")
        return
    unique_df, conflicts_df = build_unique(raw_df)
    save_outputs(raw_df, unique_df, conflicts_df)
    print(f"📦 Przeskanowano plików: {seen_files} | Kluby unikalne: {len(unique_df)}")

if __name__ == "__main__":
    main()
