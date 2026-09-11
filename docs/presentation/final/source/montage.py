"""Contact sheet of rendered slides: main deck 5x2 on top, backup slides in a smaller strip.

usage: python3 montage.py <slides_dir> <out.png>
"""
import sys
from pathlib import Path
from PIL import Image, ImageDraw

src = Path(sys.argv[1])
out = Path(sys.argv[2])
files = sorted(src.glob("slide-*.png"))
main, backup = files[:10], files[10:]

W, H = 560, 315          # thumb size (16:9)
GAP, PAD = 18, 28
cols = 5
rows_main = (len(main) + cols - 1) // cols
bw, bh = 400, 225
rows_b = (len(backup) + cols - 1) // cols if backup else 0
label_h = 26

width = PAD * 2 + cols * W + (cols - 1) * GAP
height = PAD * 2 + rows_main * (H + label_h + GAP) + (rows_b * (bh + label_h + GAP) + 40 if backup else 0)
sheet = Image.new("RGB", (width, height), "#DEDAD2")
d = ImageDraw.Draw(sheet)

def place(f, x, y, w, h, label):
    im = Image.open(f).convert("RGB").resize((w, h), Image.LANCZOS)
    d.rectangle([x - 1, y - 1, x + w, y + h], outline="#9A968E")
    sheet.paste(im, (x, y))
    d.text((x, y + h + 6), label, fill="#44505E")

y = PAD
for i, f in enumerate(main):
    r, c = divmod(i, cols)
    place(f, PAD + c * (W + GAP), y + r * (H + label_h + GAP), W, H, f"Slide {i + 1}")
y += rows_main * (H + label_h + GAP)
if backup:
    y += 14
    d.text((PAD, y), "BACKUP", fill="#44505E")
    y += 26
    for i, f in enumerate(backup):
        r, c = divmod(i, cols)
        place(f, PAD + c * (W + GAP), y + r * (bh + label_h + GAP), bw, bh, f"Backup {chr(65 + i)}")

sheet.save(out, optimize=True)
print("montage ->", out, sheet.size)
