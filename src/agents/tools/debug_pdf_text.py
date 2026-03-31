from pathlib import Path
from pdf_extract import extract_pdf_text

pdf_path = Path(r"D:\Agents\OtoCPAAi\src\agents\data\downloads\AAMkADhl_700003808336.pdf")

text = extract_pdf_text(pdf_path)

print("========== PDF TEXT START ==========")
print(text)
print("=========== PDF TEXT END ===========")