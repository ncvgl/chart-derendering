#!/usr/bin/env python3
"""Crop a region from an image. No resizing, no interpolation — original pixels only."""
import sys
from PIL import Image

if len(sys.argv) != 7:
    print("Usage: python3 crop.py input.png x1 y1 x2 y2 output.png")
    print("  Crops the rectangle (x1,y1)-(x2,y2) from input and saves to output.")
    print("  Coordinates are in pixels from top-left corner.")
    sys.exit(1)

input_path, x1, y1, x2, y2, output_path = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]), sys.argv[6]
img = Image.open(input_path)
w, h = img.size
x1, y1, x2, y2 = max(0, x1), max(0, y1), min(w, x2), min(h, y2)
crop = img.crop((x1, y1, x2, y2))
crop.save(output_path)
print(f"Cropped {input_path} ({w}x{h}) -> {output_path} ({crop.size[0]}x{crop.size[1]})")
