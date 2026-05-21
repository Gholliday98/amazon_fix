#!/usr/bin/env python3
"""
pc_generate_oem_images.py — Generate technical drawing images for OEM cutting board listings.

Reads the suppressed listings CSV, parses dimensions from each title, and renders
a 4-view technical drawing (Top, Isometric, Front, Side) for each SKU.

Output: {SKU}.jpg in the specified folder, 2000x2000px, white background.

Usage
-----
    py pc_generate_oem_images.py
    py pc_generate_oem_images.py --csv suppressed_listings_20260521_115921.csv
    py pc_generate_oem_images.py --out "X:\\Gabrielle\\cutting_board_oem_pics"
    py pc_generate_oem_images.py --thickness 0.5   # board thickness in inches (default: 0.5)
"""

import argparse
import csv
import re
import sys
from pathlib import Path

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.patches import FancyArrowPatch
    import numpy as np
except ImportError:
    print('[ERROR] matplotlib not installed. Run: pip install matplotlib')
    sys.exit(1)

# ─── Defaults ─────────────────────────────────────────────────────────────────
SCRIPT_DIR     = Path(__file__).parent
DEFAULT_OUT    = Path(r'X:\Gabrielle\cutting_board_oem_pics')
DEFAULT_THICK  = 0.5   # inches — standard commercial cutting board thickness
IMAGE_DPI      = 200
IMAGE_SIZE_IN  = (10, 10)   # inches → 2000x2000px at 200dpi

LINE_COLOR  = '#1a1a1a'
BG_COLOR    = 'white'
FILL_COLOR  = '#f5f5f5'
FONT_FAMILY = 'DejaVu Sans'


# ═══════════════════════════════════════════════════════════════════════════════
# Dimension parsing
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_inch(s: str) -> float:
    """Convert a dimension string like '11-3/4', '32.125', or '27.5' to float."""
    s = s.strip()
    # Already a decimal
    if re.match(r'^\d+\.?\d*$', s):
        return float(s)
    # Mixed number like 11-3/4 or whole-num/denom like 3/4
    m = re.match(r'^(\d+)-(\d+)/(\d+)$', s)
    if m:
        return int(m.group(1)) + int(m.group(2)) / int(m.group(3))
    m = re.match(r'^(\d+)/(\d+)$', s)
    if m:
        return int(m.group(1)) / int(m.group(2))
    return float(s)


def parse_dimensions(title: str) -> tuple[float, float] | None:
    """Extract (width, length) in inches from a listing title."""
    # Grab optional decimal override from parens: 11-3/4" (11.75") → use 11.75
    # Full pattern: <dim>" [(decimal")] W x <dim>" [(decimal")] L
    m = re.search(
        r'([\d\-/\.]+)"\s*(?:\(\s*(\d+\.?\d*)\s*"\s*\))?\s*W'
        r'\s*x\s*'
        r'([\d\-/\.]+)"\s*(?:\(\s*(\d+\.?\d*)\s*"\s*\))?\s*L',
        title, re.IGNORECASE)
    if m:
        w = float(m.group(2)) if m.group(2) else _parse_inch(m.group(1))
        l = float(m.group(4)) if m.group(4) else _parse_inch(m.group(3))
        return w, l

    # Fallback: SKU format PE10_W10L20
    m = re.search(r'W(\d+\.?\d*)L(\d+\.?\d*)', title)
    if m:
        return float(m.group(1)), float(m.group(2))

    return None


def fmt_dim(val: float) -> str:
    """Format a dimension nicely: 27.5 → '27-1/2"', 10.0 → '10"'"""
    whole = int(val)
    frac  = val - whole
    frac_map = {0.0: '', 0.25: '-1/4', 0.5: '-1/2', 0.75: '-3/4',
                0.125: '-1/8', 0.375: '-3/8', 0.625: '-5/8', 0.875: '-7/8',
                0.3125: '-5/16', 0.0625: '-1/16'}
    frac_str = frac_map.get(round(frac, 4), f'.{str(val).split(".")[-1]}' if frac else '')
    return f'{whole}{frac_str}"'


# ═══════════════════════════════════════════════════════════════════════════════
# Drawing helpers
# ═══════════════════════════════════════════════════════════════════════════════

def dim_arrow(ax, x1, y1, x2, y2, label, label_offset=(0, 0),
              fontsize=9, color=LINE_COLOR):
    """Draw a dimension line with arrows and a label."""
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle='<->', color=color, lw=1.2))
    mx = (x1 + x2) / 2 + label_offset[0]
    my = (y1 + y2) / 2 + label_offset[1]
    ax.text(mx, my, label, ha='center', va='center', fontsize=fontsize,
            fontfamily=FONT_FAMILY, color=color,
            bbox=dict(fc='white', ec='none', pad=1.5))


def dashed_line(ax, x1, y1, x2, y2, color=LINE_COLOR):
    ax.plot([x1, x2], [y1, y2], color=color, lw=0.8, ls='--')


# ═══════════════════════════════════════════════════════════════════════════════
# Four-view technical drawing
# ═══════════════════════════════════════════════════════════════════════════════

def draw_top_view(ax, W, L, T, label_w, label_l):
    ax.set_title('TOP VIEW', fontfamily=FONT_FAMILY, fontsize=11,
                 fontweight='bold', pad=10, loc='left', color=LINE_COLOR)
    ax.set_aspect('equal')

    # Scale so the shape fits nicely in a ~6x6 unit box
    scale = min(5 / max(W, L), 1.0)
    w, l = W * scale, L * scale
    margin = 1.2

    # Board rectangle
    rect = mpatches.FancyBboxPatch((margin, margin), l, w,
                                    boxstyle='square,pad=0',
                                    linewidth=1.8, edgecolor=LINE_COLOR,
                                    facecolor=FILL_COLOR)
    ax.add_patch(rect)

    # Dimension lines
    doff = 0.55
    # Width (vertical)
    dashed_line(ax, margin, margin, margin - doff, margin)
    dashed_line(ax, margin, margin + w, margin - doff, margin + w)
    dim_arrow(ax, margin - doff, margin, margin - doff, margin + w,
              f'W\n({label_w})', label_offset=(-0.35, 0), fontsize=8)

    # Length (horizontal)
    dashed_line(ax, margin, margin, margin, margin - doff)
    dashed_line(ax, margin + l, margin, margin + l, margin - doff)
    dim_arrow(ax, margin, margin - doff, margin + l, margin - doff,
              f'L\n({label_l})', label_offset=(0, -0.35), fontsize=8)

    lim = max(l, w) + margin * 2
    ax.set_xlim(-0.2, lim + 0.2)
    ax.set_ylim(-0.8, lim + 0.2)
    ax.axis('off')


def draw_isometric_view(ax, W, L, T, label_w, label_l, label_t):
    ax.set_title('ISOMETRIC VIEW', fontfamily=FONT_FAMILY, fontsize=11,
                 fontweight='bold', pad=10, loc='left', color=LINE_COLOR)

    scale = min(4.5 / max(W, L), 1.0)
    w, l, t = W * scale, L * scale, max(T * scale * 4, 0.25)

    # Isometric projection angles
    ang = np.radians(30)
    cx, cy = np.cos(ang), np.sin(ang)

    def iso(x, y, z):
        px = x * cx - y * cx
        py = x * cy + y * cy + z
        return px, py

    # 8 corners of the box
    pts = {
        'FBL': iso(0, 0, 0), 'FBR': iso(l, 0, 0),
        'FTL': iso(0, 0, t), 'FTR': iso(l, 0, t),
        'BBL': iso(0, w, 0), 'BBR': iso(l, w, 0),
        'BTL': iso(0, w, t), 'BTR': iso(l, w, t),
    }

    def face(keys, fc=FILL_COLOR, lw=1.8):
        xs = [pts[k][0] for k in keys] + [pts[keys[0]][0]]
        ys = [pts[k][1] for k in keys] + [pts[keys[0]][1]]
        ax.fill(xs, ys, fc=fc, ec=LINE_COLOR, lw=lw, zorder=2)

    # Draw faces back-to-front
    face(['BBL', 'BBR', 'BTR', 'BTL'], fc='#ececec')  # back
    face(['FBL', 'BBL', 'BTL', 'FTL'], fc='#e8e8e8')  # left
    face(['FTL', 'FTR', 'BTR', 'BTL'])                 # top

    # Dimension lines
    doff = 0.3
    # Length (L) along bottom front edge
    fx1, fy1 = pts['FBL']
    fx2, fy2 = pts['FBR']
    dim_arrow(ax, fx1, fy1 - doff, fx2, fy2 - doff,
              f'L ({label_l})', label_offset=(0, -0.25), fontsize=7.5)

    # Width (W) along bottom back-left edge
    bx1, by1 = pts['FBL']
    bx2, by2 = pts['BBL']
    dim_arrow(ax, bx1 - doff * 0.6, by1 - doff * 0.3,
              bx2 - doff * 0.6, by2 - doff * 0.3,
              f'W\n({label_w})', label_offset=(-0.4, 0), fontsize=7.5)

    # Thickness (T) on right edge
    rx1, ry1 = pts['FBR']
    rx2, ry2 = pts['FTR']
    dim_arrow(ax, rx1 + doff, ry1, rx2 + doff, ry2,
              f'T\n({label_t})', label_offset=(0.5, 0), fontsize=7.5)

    all_x = [p[0] for p in pts.values()]
    all_y = [p[1] for p in pts.values()]
    pad = 1.0
    ax.set_xlim(min(all_x) - pad, max(all_x) + pad)
    ax.set_ylim(min(all_y) - pad * 1.5, max(all_y) + pad * 0.5)
    ax.set_aspect('equal')
    ax.axis('off')


def draw_front_view(ax, W, L, T, label_l, label_t):
    ax.set_title('FRONT VIEW', fontfamily=FONT_FAMILY, fontsize=11,
                 fontweight='bold', pad=10, loc='left', color=LINE_COLOR)

    scale = min(5 / L, 1.0)
    l, t = L * scale, max(T * scale * 6, 0.18)
    margin = 0.8

    rect = mpatches.FancyBboxPatch((margin, margin), l, t,
                                    boxstyle='square,pad=0',
                                    linewidth=1.8, edgecolor=LINE_COLOR,
                                    facecolor=FILL_COLOR)
    ax.add_patch(rect)

    doff = 0.35
    # Length
    dashed_line(ax, margin, margin, margin, margin - doff)
    dashed_line(ax, margin + l, margin, margin + l, margin - doff)
    dim_arrow(ax, margin, margin - doff, margin + l, margin - doff,
              f'L ({label_l})', label_offset=(0, -0.25), fontsize=8)

    # Thickness
    dashed_line(ax, margin + l, margin, margin + l + doff, margin)
    dashed_line(ax, margin + l, margin + t, margin + l + doff, margin + t)
    dim_arrow(ax, margin + l + doff, margin, margin + l + doff, margin + t,
              f'T\n({label_t})', label_offset=(0.4, 0), fontsize=8)

    ax.set_xlim(0, margin + l + 1.2)
    ax.set_ylim(margin - 0.8, margin + t + 0.5)
    ax.axis('off')


def draw_side_view(ax, W, L, T, label_w, label_t):
    ax.set_title('SIDE VIEW', fontfamily=FONT_FAMILY, fontsize=11,
                 fontweight='bold', pad=10, loc='left', color=LINE_COLOR)

    scale = min(5 / W, 1.0)
    w, t = W * scale, max(T * scale * 6, 0.18)
    margin = 0.8

    rect = mpatches.FancyBboxPatch((margin, margin), w, t,
                                    boxstyle='square,pad=0',
                                    linewidth=1.8, edgecolor=LINE_COLOR,
                                    facecolor=FILL_COLOR)
    ax.add_patch(rect)

    doff = 0.35
    # Width
    dashed_line(ax, margin, margin, margin, margin - doff)
    dashed_line(ax, margin + w, margin, margin + w, margin - doff)
    dim_arrow(ax, margin, margin - doff, margin + w, margin - doff,
              f'W ({label_w})', label_offset=(0, -0.25), fontsize=8)

    # Thickness
    dashed_line(ax, margin + w, margin, margin + w + doff, margin)
    dashed_line(ax, margin + w, margin + t, margin + w + doff, margin + t)
    dim_arrow(ax, margin + w + doff, margin, margin + w + doff, margin + t,
              f'T\n({label_t})', label_offset=(0.4, 0), fontsize=8)

    ax.set_xlim(0, margin + w + 1.2)
    ax.set_ylim(margin - 0.8, margin + t + 0.5)
    ax.axis('off')


# ═══════════════════════════════════════════════════════════════════════════════
# Generate one image per SKU
# ═══════════════════════════════════════════════════════════════════════════════

def generate_image(sku: str, title: str, thickness: float, out_dir: Path) -> bool:
    dims = parse_dimensions(title)
    if not dims:
        return False
    W, L = dims
    T = thickness

    label_w = fmt_dim(W)
    label_l = fmt_dim(L)
    label_t = fmt_dim(T)

    fig, axes = plt.subplots(2, 2, figsize=IMAGE_SIZE_IN,
                              facecolor=BG_COLOR,
                              gridspec_kw={'hspace': 0.35, 'wspace': 0.25})
    fig.patch.set_facecolor(BG_COLOR)

    # Add a thin border separator between the 4 panels
    for ax in axes.flat:
        ax.set_facecolor(BG_COLOR)

    draw_top_view(axes[0][0], W, L, T, label_w, label_l)
    draw_isometric_view(axes[0][1], W, L, T, label_w, label_l, label_t)
    draw_front_view(axes[1][0], W, L, T, label_l, label_t)
    draw_side_view(axes[1][1], W, L, T, label_w, label_t)

    # Thin divider lines between quadrants
    fig.add_artist(plt.Line2D([0.5, 0.5], [0.02, 0.98],
                               transform=fig.transFigure,
                               color='#cccccc', lw=0.8))
    fig.add_artist(plt.Line2D([0.02, 0.98], [0.5, 0.5],
                               transform=fig.transFigure,
                               color='#cccccc', lw=0.8))

    dest = out_dir / f'{sku}.jpg'
    fig.savefig(dest, dpi=IMAGE_DPI, bbox_inches='tight',
                facecolor=BG_COLOR, format='jpeg')
    plt.close(fig)
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# Find latest CSV
# ═══════════════════════════════════════════════════════════════════════════════

def find_csv() -> Path | None:
    candidates = sorted(SCRIPT_DIR.glob('suppressed_listings_*.csv'), reverse=True)
    return candidates[0] if candidates else None


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description='Generate OEM cutting board technical drawings.')
    parser.add_argument('--csv', help='Path to suppressed listings CSV')
    parser.add_argument('--out', default=str(DEFAULT_OUT),
                        help=r'Output folder (default: X:\Gabrielle\cutting_board_oem_pics)')
    parser.add_argument('--thickness', type=float, default=DEFAULT_THICK,
                        help=f'Board thickness in inches (default: {DEFAULT_THICK})')
    args = parser.parse_args()

    csv_path = Path(args.csv) if args.csv else find_csv()
    if not csv_path or not csv_path.exists():
        print('[ERROR] Could not find suppressed listings CSV. Use --csv to specify one.')
        sys.exit(1)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f'Output folder: {out_dir}')
    print(f'Reading from:  {csv_path}')
    print(f'Thickness:     {args.thickness}"\n')

    rows   = list(csv.DictReader(csv_path.open(encoding='utf-8-sig')))
    total  = len(rows)
    done   = 0
    failed = []

    for i, row in enumerate(rows, 1):
        sku   = row.get('sku', row.get('SKU', '')).strip()
        title = row.get('title', row.get('Product name', '')).strip()

        dest = out_dir / f'{sku}.jpg'
        if dest.exists():
            print(f'[{i:>3}/{total}] SKIP  {sku}')
            done += 1
            continue

        ok = generate_image(sku, title, args.thickness, out_dir)
        if ok:
            print(f'[{i:>3}/{total}] OK    {sku}')
            done += 1
        else:
            print(f'[{i:>3}/{total}] FAIL  {sku}  (could not parse dimensions from: {title[:60]})')
            failed.append(sku)

    print(f'\n{"="*50}')
    print(f'  Done. {done}/{total} images generated in:')
    print(f'  {out_dir}')
    if failed:
        print(f'  {len(failed)} failed (no dimensions found): {", ".join(failed)}')


if __name__ == '__main__':
    main()
