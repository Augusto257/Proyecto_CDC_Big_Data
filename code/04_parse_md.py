"""04_parse_md.py - Parser v2: md/*.md -> master + detalle."""
import glob, os, re
import pandas as pd

MD_DIR = "./md"
OUT = "./out"

def get_anio_semana(txt, fname):
    m = re.search(r"SE\s*0?(\d{1,2})\s*[-â€“]?\s*20(\d{2})", txt[:3000])
    if m: return 2000+int(m.group(2)), int(m.group(1))
    m2 = re.search(r"20(\d{2}).*?(\d{1,2})", fname)
    return (2000+int(m2.group(1)), 0) if m2 else (2026, 0)

def norm(s):
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn").lower()

def section_window(txt, keywords, size=6000):
    low = norm(txt)
    for kw in keywords:
        i = low.find(norm(kw))
        if i >= 0: return txt[max(0,i-500):i+size]
    return ""

def num_in(win, pats):
    for p in pats:
        m = re.search(p, win, re.IGNORECASE)
        if m and m.group(1):
            try: return int(m.group(1).replace(",", "").replace(".", ""))
            except: pass
    return None

def dec_in(win, pats):
    for p in pats:
        m = re.search(p, win, re.IGNORECASE)
        if m and m.group(1):
            try: return float(m.group(1).replace(",", "."))
            except: pass
    return None

DISEASES = {
 "DENGUE": ["dengue"],
 "CARRION": ["carrion", "bartonel"],
}

master, detail = [], []
for md in sorted(glob.glob(os.path.join(MD_DIR, "*.md"))):
    txt = open(md, encoding="utf-8").read()
    anio, sem = get_anio_semana(txt, md)
    base = os.path.basename(md)
    for enf, kws in DISEASES.items():
        counts = sum(len(re.findall(k, norm(txt))) for k in kws)
        tiene = 1 if counts >= 10 else 0
        win = section_window(txt, kws, 8000) if tiene else txt[:3000]
        low = txt.lower()
        row = {
            "anio": anio, "semana_epidemiologica": sem, "enfermedad": enf,
            "tiene_seccion": tiene,
            "casos_acumulados": num_in(win, [r"se han notificado\s*(\d+)\s*casos", r"acumulad\w*\s*[:\-]?\s*(\d[\d\.,]*)", r"total\w*\s*(\d+)\s*casos"]),
            "casos_confirmados": num_in(win, [r"(\d+)\s*fueron confirmados", r"(\d+)\s*confirmados", r"confirmad\w*\s*[:\-]?\s*(\d+)"]),
            "casos_probables": num_in(win, [r"(\d+)\s*se clasificaron como probables", r"(\d+)\s*probables", r"probable\w*\s*[:\-]?\s*(\d+)"]),
            "defunciones": num_in(win, [r"se report.?\s*(\d+)\s*defunci", r"(\d+)\s*defunci", r"(\d+)\s*fallecid"]),
            "letalidad": dec_in(win, [r"letalidad\s*[:\-]?\s*([\d\.,]+)", r"tasa.*?([\d\.,]+)\s*por 100 mil"]),
            "tendencia": ("incremento" if any(w in win.lower() for w in ["incremento","aumento"]) else "estable"),
            "menciona_lluvias": int("lluvia" in low), "menciona_temp": int("temperatura" in low),
            "menciona_nino": int("niÃ±o" in low or "nino" in low), "menciona_criaderos": int(any(w in low for w in ["criadero","aedes","lutzomyia"])),
        }
        master.append(row)
    lines = txt.splitlines()
    for i, ln in enumerate(lines):
        if ln.strip().startswith("|") and "epartamento" in ln:
            ctx = norm("\n".join(lines[max(0,i-30):i]))
            enf_tab = "CARRION" if ("carrion" in ctx or "bartonel" in ctx) and "dengue" not in ctx[-500:] else "DENGUE"
            for j in range(i+2, min(i+40, len(lines))):
                r = lines[j]
                if not r.strip().startswith("|"): break
                cells = [c.strip() for c in r.strip().strip("|").split("|")]
                if len(cells) < 3 or set(cells[0]) <= set("- :"): continue
                dep = re.sub(r"[^A-ZÃ‘ÃÃ‰ÃÃ“Ãš ]", "", cells[0].upper()).strip()
                direc = re.sub(r"[^A-ZÃ‘ÃÃ‰ÃÃ“Ãš ]", "", cells[1].upper()).strip() if len(cells) > 3 else dep
                if len(dep) < 3 or dep.startswith("---") or dep in ("TOTAL GENERAL", "PERU", "PERÃš"): continue
                nums = re.findall(r"\d+", " ".join(cells[2:]))
                if nums:
                    detail.append({"anio_boletin": anio, "semana": sem, "enfermedad": enf_tab,
                                   "departamento": dep, "direccion_salud": direc if direc else dep,
                                   "casos_se_actual": int(nums[-1])})
    DEPS = ["AMAZONAS","ANCASH","APURIMAC","AREQUIPA","AYACUCHO","CAJAMARCA","CALLAO","CUSCO","HUANCAVELICA","HUANUCO","ICA","JUNIN","LA LIBERTAD","LAMBAYEQUE","LIMA","LORETO","MADRE DE DIOS","MOQUEGUA","PASCO","PIURA","PUNO","SAN MARTIN","TACNA","TUMBES","UCAYALI"]
    if not any(d["anio_boletin"] == anio and d["semana"] == sem for d in detail):
        found = sorted(set(re.findall("|".join(DEPS), txt.upper())))
        for dep in found:
            detail.append({"anio_boletin": anio, "semana": sem, "enfermedad": "DENGUE",
                           "departamento": dep, "direccion_salud": dep, "casos_se_actual": None})

mdf = pd.DataFrame(master)
ddf = pd.DataFrame(detail).drop_duplicates() if detail else pd.DataFrame(columns=["anio_boletin","semana","departamento"])
os.makedirs(OUT, exist_ok=True)
mdf.to_csv(f"{OUT}/boletines_master.csv", index=False, sep=";", encoding="utf-8-sig")
mdf.to_parquet(f"{OUT}/boletines_master.parquet", index=False)
ddf.to_csv(f"{OUT}/boletines_detalle.csv", index=False, sep=";", encoding="utf-8-sig")
ddf.to_parquet(f"{OUT}/boletines_detalle.parquet", index=False)
print(f"MASTER {len(mdf)} filas:\n", mdf[["anio","semana_epidemiologica","enfermedad","tiene_seccion","casos_acumulados","casos_confirmados","defunciones"]].to_string())
print(f"\nDETALLE {len(ddf)} filas")
print(ddf.head(15).to_string())
print("OK -> subir out/boletines_master.* y boletines_detalle.* a processed/")
