# The logo, taken off its background.
#
#     python3 tools/logo.py <picture> [docs/rillmq.png]
#
# What comes out is checked in, so nothing has to be run to read the README.
# It came as a JPEG on a near-white vignette, and making that transparent is
# two problems rather than one: which pixels are background, and what colour
# the edge pixels would have been without it. Getting the second wrong is what
# leaves a pale rim around every letter on a dark page.
#
# Wants pillow and numpy.
import sys
import warnings

from PIL import Image, ImageFilter
import numpy as np

if len(sys.argv) < 2:
    sys.exit('usage: python3 tools/logo.py <picture> [out.png]')
SRC = sys.argv[1]
OUT = sys.argv[2] if len(sys.argv) > 2 else 'docs/rillmq.png'
WIDE = 840   # twice what the README asks for, so it holds up on a dense screen

im = Image.open(SRC).convert('RGB')
w, h = im.size
a = np.asarray(im).astype(np.float32)

# The background is estimated, not assumed: a max filter on a small copy takes
# the lightest thing nearby, which is background everywhere the logo is not.
small = im.resize((w // 8, h // 8), Image.LANCZOS).filter(ImageFilter.MaxFilter(51)).filter(ImageFilter.GaussianBlur(12))
bg = np.asarray(small.resize((w, h), Image.BICUBIC)).astype(np.float32)

d = np.clip(bg - a, 0, None).max(axis=2)   # distance from background; noise reaches 11

# Pixels that are certainly logo, and their colours grown outward over the
# edge pixels, so that an edge takes the colour of the stroke it belongs to
# rather than the colour it was blended into.
core = d > 60
c = np.where(core[..., None], a, np.nan)
for _ in range(6):
    pad = np.pad(c, ((1, 1), (1, 1), (0, 0)), constant_values=np.nan)
    stack = np.stack([pad[y:y + h, x:x + w] for y in range(3) for x in range(3)])
    with warnings.catch_warnings():   # a pixel with no known neighbour yet
        warnings.simplefilter('ignore', RuntimeWarning)
        mean = np.nanmean(stack, axis=0)
    c = np.where(np.isnan(c), mean, c)
c = np.where(np.isnan(c), bg, c)

# alpha from the compositing equation itself: observed = a*C + (1-a)*BG.
denom = np.maximum(np.clip(bg - c, 0, None).max(axis=2), 20.0)
alpha = np.clip(d / denom, 0, 1)
alpha *= np.clip((d - 11.0) / 8.0, 0, 1)   # anything inside the noise is background
alpha[alpha < 0.02] = 0

# The source has two faint vertical bands down its sides, three per cent
# opaque after all this and invisible until the page behind is dark. The crop
# is taken from what is solidly logo, with a margin, so the bands fall outside
# it; anything left below four per cent is film rather than picture.
alpha[alpha < 0.04] = 0
solid = alpha > 0.16
ys, xs = np.where(solid)
pad = 12
top, bottom = max(ys.min() - pad, 0), min(ys.max() + pad + 1, h)
left, right = max(xs.min() - pad, 0), min(xs.max() + pad + 1, w)

out = np.dstack([np.clip(c, 0, 255), alpha * 255]).astype(np.uint8)
img = Image.fromarray(out, 'RGBA').crop((left, top, right, bottom))
print('trimmed', img.size)

logo = img.resize((WIDE, round(img.size[1] * WIDE / img.size[0])), Image.LANCZOS)

# The resampling puts a little of what it mixed back: one and two per cent
# alpha where there should be none. Below two and a half per cent nothing is
# being drawn, so it is cleared rather than shipped.
la = np.asarray(logo).copy()
la[..., 3][la[..., 3] < 6] = 0
logo = Image.fromarray(la, 'RGBA')
logo.save(OUT, optimize=True)
print('saved', logo.size)
