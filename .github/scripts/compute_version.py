#!/usr/bin/env python3
"""Compute the next release version for the release workflow.

Inputs (environment):
  CHANNEL       'official' | 'prerelease'
  RELEASE_TYPE  'major' | 'minor' | 'patch'  (the part to bump)
  LABEL         prerelease label, e.g. 'preview' / 'rc' (channel=prerelease only)

The "last release" is the highest-semver GitHub release (official OR prerelease),
by semver precedence — NOT by date. Rules:

  official:
    - no prior release        -> Directory.Build.props VersionPrefix (no bump)
    - last is a prerelease     -> finalize it: drop the suffix, keep the base (no bump)
    - last is an official      -> bump <RELEASE_TYPE>

  prerelease:
    - no prior release         -> props base + '-<label>.1'
    - last is a prerelease     -> keep the base, next '<label>.N' (N = highest for base+label + 1)
    - last is an official      -> bump <RELEASE_TYPE>, then '-<label>.1'

Outputs (written as key=value lines to stdout -> $GITHUB_OUTPUT):
  version, version_prefix, version_suffix, tag, is_prerelease
"""
import json
import os
import re
import subprocess
import sys

PROPS = "Directory.Build.props"
TAG_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)(?:-(.+))?$")
SUFFIX_RE = re.compile(r"^(?P<label>.*?)\.?(?P<num>\d+)$")


def log(msg):
	print(msg, file=sys.stderr)


def read_props_base(path=PROPS):
	with open(path, encoding="utf-8") as f:
		txt = f.read()
	m = re.search(r"<VersionPrefix>\s*(\d+)\.(\d+)\.(\d+)\s*</VersionPrefix>", txt)
	if not m:
		log(f"::error::VersionPrefix (X.Y.Z) not found in {path}")
		sys.exit(1)
	return (int(m[1]), int(m[2]), int(m[3]))


def parse(tag):
	m = TAG_RE.match((tag or "").strip())
	if not m:
		return None
	major, minor, patch, pre = int(m[1]), int(m[2]), int(m[3]), m[4]
	label, num = None, None
	if pre is not None:
		sm = SUFFIX_RE.match(pre)
		if sm:
			label = sm.group("label").rstrip(".") or pre
			num = int(sm.group("num"))
		else:
			label, num = pre, 0
	return {"major": major, "minor": minor, "patch": patch,
	        "pre": pre, "label": label, "num": num, "tag": tag.strip()}


def sort_key(r):
	# Official (pre is None) ranks ABOVE any prerelease sharing the same base.
	return (r["major"], r["minor"], r["patch"],
	        1 if r["pre"] is None else 0, r["label"] or "", r["num"] or 0)


def gh_releases():
	try:
		out = subprocess.run(
			["gh", "release", "list", "--limit", "500", "--json", "tagName,isPrerelease"],
			capture_output=True, text=True, check=True).stdout
	except Exception as e:  # gh missing / unauthenticated / no repo -> treat as none
		log(f"::warning::could not list GitHub releases ({e}); treating as no prior release")
		return []
	parsed = [p for d in json.loads(out or "[]") if (p := parse(d.get("tagName", "")))]
	return parsed


def main():
	channel = os.environ.get("CHANNEL", "prerelease").strip()
	rtype = os.environ.get("RELEASE_TYPE", "patch").strip()
	label = (os.environ.get("LABEL", "preview").strip() or "preview")
	if channel not in ("official", "prerelease"):
		log(f"::error::CHANNEL must be official|prerelease, got '{channel}'")
		sys.exit(1)
	if rtype not in ("major", "minor", "patch"):
		log(f"::error::RELEASE_TYPE must be major|minor|patch, got '{rtype}'")
		sys.exit(1)

	props = read_props_base()
	rels = gh_releases()
	last = max(rels, key=sort_key) if rels else None

	def bump(base):
		M, m, p = base
		return {"major": (M + 1, 0, 0), "minor": (M, m + 1, 0), "patch": (M, m, p + 1)}[rtype]

	if last is None:
		base = props                                   # rule 3 — start from props
	elif last["pre"] is not None:
		base = (last["major"], last["minor"], last["patch"])  # continue / finalize the prerelease base
	else:
		base = bump((last["major"], last["minor"], last["patch"]))  # bump the official

	prefix = f"{base[0]}.{base[1]}.{base[2]}"

	if channel == "official":
		suffix, version, is_pre = "", prefix, "false"
	else:
		n = 1
		for r in rels:
			if r["pre"] is not None and (r["major"], r["minor"], r["patch"]) == base and r["label"] == label:
				n = max(n, r["num"] + 1)
		suffix = f"{label}.{n}"
		version = f"{prefix}-{suffix}"
		is_pre = "true"

	tag = f"v{version}"
	log(f"::notice::last release = {last['tag'] if last else '(none)'} | "
	    f"channel={channel} type={rtype} -> {version}")

	for k, v in (("version", version), ("version_prefix", prefix),
	             ("version_suffix", suffix), ("tag", tag), ("is_prerelease", is_pre)):
		print(f"{k}={v}")


if __name__ == "__main__":
	main()
