"""03c_markitdown.py - Convierte los 11 PDFs a Markdown con markitdown (gratuito, MIT).
pip install 'markitdown[pdf]'
Luego parsea los .md para tu esquema boletines_structured.
"""
import glob, os
from markitdown import MarkItDown

md = MarkItDown(enable_plugins=False)
os.makedirs("./md", exist_ok=True)

for pdf in sorted(glob.glob("./pdfs/*.pdf")):
    out = os.path.join("./md", os.path.splitext(os.path.basename(pdf))[0] + ".md")
    print(f"{pdf} -> {out}")
    try:
        result = md.convert_local(pdf)
        with open(out, "w", encoding="utf-8") as f:
            f.write(result.markdown)
        print(f"  OK {len(result.markdown)} chars")
    except Exception as e:
        print(f"  ERROR {e}")

print("Listo: ./md/*.md -> sÃºbelos a gs://cdc-epi-lake-2026/raw/boletines_md/ y parsea tablas con regex.")
