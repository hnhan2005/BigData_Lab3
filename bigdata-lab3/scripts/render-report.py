#!/usr/bin/env python3
"""Render the checked-in Markdown report to a self-contained printable HTML file."""

import html
import re
import sys
from pathlib import Path


def inline(text):
    escaped = html.escape(text)
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)
    return escaped


def render(lines):
    out, paragraph, listing = [], [], False

    def flush_paragraph():
        if paragraph:
            out.append("<p>" + " ".join(inline(x.strip()) for x in paragraph) + "</p>")
            paragraph[:] = []

    index = 0
    while index < len(lines):
        line = lines[index].rstrip("\n")
        if line.startswith("#"):
            flush_paragraph()
            if listing:
                out.append("</ul>"); listing = False
            level = len(line) - len(line.lstrip("#"))
            out.append(f"<h{level}>{inline(line[level:].strip())}</h{level}>")
        elif line.startswith("> "):
            flush_paragraph(); out.append("<blockquote>" + inline(line[2:]) + "</blockquote>")
        elif line.startswith("- "):
            flush_paragraph()
            if not listing:
                out.append("<ul>"); listing = True
            out.append("<li>" + inline(line[2:]) + "</li>")
        elif line.startswith("|") and index + 1 < len(lines) and re.match(r"^\|[| :\-]+\|$", lines[index + 1].strip()):
            flush_paragraph()
            if listing:
                out.append("</ul>"); listing = False
            headers = [x.strip() for x in line.strip("|").split("|")]
            index += 2
            rows = []
            while index < len(lines) and lines[index].startswith("|"):
                rows.append([x.strip() for x in lines[index].strip().strip("|").split("|")])
                index += 1
            index -= 1
            out.append("<table><thead><tr>" + "".join("<th>" + inline(x) + "</th>" for x in headers) + "</tr></thead><tbody>")
            for row in rows:
                out.append("<tr>" + "".join("<td>" + inline(x) + "</td>" for x in row) + "</tr>")
            out.append("</tbody></table>")
        elif not line.strip():
            flush_paragraph()
            if listing:
                out.append("</ul>"); listing = False
        else:
            paragraph.append(line)
        index += 1
    flush_paragraph()
    if listing:
        out.append("</ul>")
    return "\n".join(out)


if __name__ == "__main__":
    source = Path(sys.argv[1] if len(sys.argv) > 1 else "docs/Report.md")
    target = Path(sys.argv[2] if len(sys.argv) > 2 else "docs/Report.html")
    body = render(source.read_text(encoding="utf-8").splitlines(True))
    document = f"""<!doctype html><html lang="vi"><head><meta charset="utf-8"><style>
@page {{ size: A4; margin: 18mm 16mm; }}
body {{ font: 11pt/1.48 Arial, sans-serif; color:#172033; max-width: 180mm; margin:auto; }}
h1 {{ font-size:22pt; color:#153b66; }} h2 {{ font-size:16pt; color:#1d527f; border-bottom:1px solid #ccd8e5; }}
h3 {{ font-size:12.5pt; color:#294e70; }} code {{ background:#eef2f6; padding:1px 3px; }}
blockquote {{ background:#fff4d6; border-left:4px solid #e1a929; margin:0; padding:8px 12px; }}
table {{ border-collapse:collapse; width:100%; }} th,td {{ border:1px solid #aebdca; padding:5px; text-align:left; }}
thead {{ background:#e9f0f7; }} h2,h3,table {{ break-inside:avoid; }}
</style></head><body>{body}</body></html>"""
    target.write_text(document, encoding="utf-8")
