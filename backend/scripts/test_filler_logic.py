"""Minimal test - no app imports, just copy the function logic."""
import re

def _is_dot_leader_or_filler(text: str) -> bool:
    s = (text or "").strip()
    if not s:
        return True
    alpha_count = sum(1 for ch in s if ch.isalpha())
    if alpha_count >= 5:
        return False
    if re.fullmatch(r"[\._\-=:;\s]+", s):
        non_space = sum(1 for ch in s if not ch.isspace())
        return non_space >= 4
    if re.search(r"\.{5,}|_{4,}|-{4,}", s):
        return True
    return False

test_cases = [
    ("Ho Ten TV1: Ha Hoang Phuc .......MSSV: 222683.......", False),
    ("Ten de tai: Website hoc lap trinh - CodequestAI.......", False),
    ("Tom tat noi dung de tai: ...", False),
    ("Phan tich thiet ke he thong: Co ......", False),
    ("............................................", True),
    ("____________________________", True),
    ("", True),
    ("1", False),
    ("AB", False),
    ("....", True),
]

all_pass = True
for text, expected in test_cases:
    result = _is_dot_leader_or_filler(text)
    status = "PASS" if result == expected else "FAIL"
    if status == "FAIL":
        all_pass = False
    print(f"  [{status}] expected={expected} got={result} | {repr(text[:60])}")

print(f"\nAll unit tests: {'PASS' if all_pass else 'FAIL'}")
