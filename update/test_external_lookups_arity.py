"""Every caller unpacks lookups() into as many names as it returns.

#256 widened lookups() to 4 values while #260 (written before it) still unpacked 2; each PR
passed alone and the merged nightly failed with "too many values to unpack". Run: python3 update/test_external_lookups_arity.py
"""
import ast, pathlib, sys

src = pathlib.Path(__file__).with_name("retrieveExternalArticles.py").read_text()
tree = ast.parse(src)
fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "lookups")
returns = [n.value for n in ast.walk(fn) if isinstance(n, ast.Return) and n.value is not None]
arity = {len(r.elts) if isinstance(r, ast.Tuple) else 1 for r in returns}
assert len(arity) == 1, f"lookups() returns tuples of different sizes: {arity}"
want = arity.pop()

bad = []
for n in ast.walk(tree):
    if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call) and getattr(n.value.func, "id", None) == "lookups":
        t = n.targets[0]
        got = len(t.elts) if isinstance(t, ast.Tuple) else 1
        if got != want:
            bad.append(f"line {n.lineno}: unpacks {got}, lookups() returns {want}")
assert not bad, "; ".join(bad)
print(f"PASS: every lookups() call unpacks {want} values")
sys.exit(0)
