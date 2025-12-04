import os

root = r"C:\Users\ARNULFO\Documents\GitHub\BigDataApp_2025_s2_MPBM"

print("BUSCANDO ARCHIVOS QUE CONTIENEN 'MongoDB'...\n")

for folder, dirs, files in os.walk(root):
    for fname in files:
        if "mongo" in fname.lower():
            full = os.path.join(folder, fname)
            print(">>", full)