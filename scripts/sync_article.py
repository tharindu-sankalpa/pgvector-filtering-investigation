"""Keep the full Medium draft synchronized with the canonical README."""
from pathlib import Path
import re
ROOT = Path(__file__).resolve().parents[1]
s = (ROOT / 'README.md').read_text()
# Article lives one directory deeper; preserve all text and external links.
s = re.sub(r'(?<=\]\()((?:doc|src|scripts|infrastructure|kube|data)/[^)]+)', r'../\1', s)
s = s.replace('src="doc/', 'src="../doc/')
(ROOT / 'doc/article.md').write_text(s)
