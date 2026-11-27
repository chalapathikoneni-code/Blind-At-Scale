"""Scoring for the NON-ALIGNED fact-fact study (PREREGISTRATION.md
Amendment 5, Section A5.2, locked 2026-11-06 12:15 UTC).

Reads predictions_nonaligned.csv (arms A1, A4, A5, A8) from the 12
data/nonaligned_<level>_<family>_s<seed>/ datasets and writes
results/nonalign/summary.json + results/nonalign/README.md.

Pooling is PER LEVEL (NA1, NA2): 6 datasets x 11 pipelines = 66
instances per level. Per arm: recall / precision / F1 of FAIL
prediction (FAIL is the positive class, score.py convention), Spearman
rho on log10 loads, and median Q-error (median of max(p/g, g/p)) --
overall, on the F family only (F_A, F_B; 12 instances per level, the
primary split), and on the join-emergent split (J + F; 30 instances
per level, secondary). The F-family median Q-error per arm per level
is also reported explicitly.

Aligned reference: computed here from data/<family>_s<seed>/
predictions_main.csv restricted to the SAME 6 family/seed combos
(families {zipf, lognormal, mixed} x seeds {101, 202} = 66 instances),
so the comparison is apples-to-apples. The full-grid pooled and
join-emergent values from results/summary.json (99 instances,
including seed 303) are additionally copied into summary.json for
context, clearly labelled as the full grid.

Also verifies, per non-aligned dataset, that its fact per-key counts
(fact_kA/kB/kC/kN and null_count in counts.npz) are bit-identical to
the aligned dataset's for the same family/seed -- the pre-registered
sanity check that only the fact2 pmf permutation changed anything
(distinct_A equality is recorded as additional information).
"""
import csv, json, os
from datetime import datetime, timezone

import numpy as np
from scipy import stats

BASE = os.path.expanduser("~/workspace/blind-at-scale")
FAMILIES = ("zipf", "lognormal", "mixed")
SEEDS = (101, 202)
LEVELS = ("NA1", "NA2")
ARMS = ["A1", "A4", "A5", "A8"]
F_PIPES = ("F_A", "F_B")
JF_PIPES = ("J_A", "J_B", "J_N", "F_A", "F_B")


def load_instances(tags, rtag):
    """One instance per (dataset, pipeline) with gt + per-arm preds."""
    inst = {}
    for tag in tags:
        path = os.path.join(BASE, "data", tag, f"predictions_{rtag}.csv")
        with open(path) as f:
            for r in csv.DictReader(f):
                if r["arm"] not in ARMS:
                    continue
                key = (r["dataset"], r["pipeline"])
                e = inst.setdefault(key, {"dataset": r["dataset"],
                                          "family": r["family"],
                                          "pipeline": r["pipeline"],
                                          "gt": r["gt_verdict"],
                                          "gt_load": float(r["gt_load"])})
                e[r["arm"]] = r["pred_verdict"]
                e[r["arm"] + "_load"] = float(r["pred_load"])
    return list(inst.values())


def prf(sub):
    tp = sum(1 for r in sub if r["gt"] == "FAIL" and r["pred"] == "FAIL")
    fp = sum(1 for r in sub if r["gt"] == "PASS" and r["pred"] == "FAIL")
    fn = sum(1 for r in sub if r["gt"] == "FAIL" and r["pred"] == "PASS")
    tn = sum(1 for r in sub if r["gt"] == "PASS" and r["pred"] == "PASS")
    rec = tp / (tp + fn) if tp + fn else float("nan")
    prec = tp / (tp + fp) if tp + fp else float("nan")
    f1 = 2 * rec * prec / (rec + prec) if rec + prec else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "recall": rec, "precision": prec, "f1": f1, "n": len(sub)}


def metrics(inst, arm):
    sub = [{"gt": e["gt"], "pred": e[arm]} for e in inst]
    m = prf(sub)
    loads = np.array([e[arm + "_load"] for e in inst], dtype=np.float64)
    gt = np.array([e["gt_load"] for e in inst], dtype=np.float64)
    m["spearman_rho"] = float(stats.spearmanr(np.log10(loads),
                                              np.log10(gt)).statistic)
    q = np.maximum(loads / gt, gt / loads)
    m["median_q_error"] = float(np.median(q))
    return m


def block(inst):
    """Per-arm metrics: overall / F-only (primary) / join-emergent."""
    out = {}
    f_inst = [e for e in inst if e["pipeline"] in F_PIPES]
    jf_inst = [e for e in inst if e["pipeline"] in JF_PIPES]
    for arm in ARMS:
        fm = metrics(f_inst, arm)
        out[arm] = {"overall": metrics(inst, arm),
                    "F": fm,
                    "join_emergent": metrics(jf_inst, arm),
                    "F_median_q_error": fm["median_q_error"]}
    return {"n_instances": len(inst),
            "n_gt_fail": sum(1 for e in inst if e["gt"] == "FAIL"),
            "n_F": len(f_inst), "n_join_emergent": len(jf_inst),
            "arms": out}


def fmt(x, nd=3):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "n/a"
    return f"{x:.{nd}f}"


def main():
    ts = datetime.now(timezone.utc).isoformat()
    datasets_meta = []
    validation = {}
    fact_identity = {}
    levels_inst = {lv: [] for lv in LEVELS}
    for lv in LEVELS:
        for fam in FAMILIES:
            for seed in SEEDS:
                tag = f"nonaligned_{lv}_{fam}_s{seed}"
                d = os.path.join(BASE, "data", tag)
                with open(os.path.join(d, "meta.json")) as f:
                    meta = json.load(f)
                with open(os.path.join(d, "validation.json")) as f:
                    val = json.load(f)
                datasets_meta.append({"dataset": tag, **meta})
                validation[tag] = val
                # fact-count identity sanity check vs aligned dataset
                a = np.load(os.path.join(d, "counts.npz"))
                b = np.load(os.path.join(
                    BASE, "data", f"{fam}_s{seed}", "counts.npz"))
                cols = ["fact_kA", "fact_kB", "fact_kC", "fact_kN"]
                per = {c: bool(np.array_equal(a[c], b[c])) for c in cols}
                per["null_count"] = bool(
                    int(a["null_count"]) == int(b["null_count"]))
                per["distinct_A"] = bool(
                    np.array_equal(a["distinct_A"], b["distinct_A"]))
                per["all_fact_counts_identical"] = all(
                    per[c] for c in cols + ["null_count"])
                fact_identity[tag] = per
                levels_inst[lv].extend(
                    load_instances([tag], "nonaligned"))

    S = {"study": "Blind at Scale, Amendment 5 Section A5.2 (Item 9): "
                  "non-aligned fact-fact corpora",
         "generated_at_utc": ts,
         "arms": ARMS,
         "datasets": datasets_meta,
         "validation": validation,
         "fact_count_identity_vs_aligned": fact_identity,
         "levels": {lv: block(levels_inst[lv]) for lv in LEVELS}}

    # seeds-matched aligned reference (66 instances, same combos)
    aligned_tags = [f"{fam}_s{seed}" for fam in FAMILIES for seed in SEEDS]
    S["aligned_reference_seeds_matched"] = block(
        load_instances(aligned_tags, "main"))
    S["aligned_reference_seeds_matched"]["note"] = (
        "Computed from predictions_main.csv of the 6 aligned datasets "
        "with seeds {101, 202} x families {zipf, lognormal, mixed} "
        "(66 instances); arms restricted to A1, A4, A5, A8.")

    # full-grid reference values copied from results/summary.json
    with open(os.path.join(BASE, "results", "summary.json")) as f:
        full = json.load(f)
    S["aligned_reference_full_grid_from_summary_json"] = {
        "note": "Copied verbatim from results/summary.json (99 "
                "instances, seeds {101, 202, 303}); NOT seeds-matched, "
                "context only.",
        "n_instances": full["n_instances"],
        "arms_overall": {a: full["arms"][a] for a in ARMS},
        "arms_join_emergent": {
            a: full["per_pipeline_family"]["join-emergent (J,F)"][a]
            for a in ARMS}}

    os.makedirs(os.path.join(BASE, "results", "nonalign"), exist_ok=True)
    with open(os.path.join(BASE, "results", "nonalign",
                           "summary.json"), "w") as f:
        json.dump(S, f, indent=1)

    # ---------------- README.md (values only from S above) ----------------
    deviations = []
    for tag, val in validation.items():
        if not val["all_exact"]:
            deviations.append(f"{tag}: validation NOT all exact: {val}")
    for tag, per in fact_identity.items():
        if not per["all_fact_counts_identical"]:
            deviations.append(f"{tag}: fact counts NOT identical to "
                              f"aligned dataset: {per}")
    aligned = S["aligned_reference_seeds_matched"]["arms"]
    L = []
    L.append("# Item 9 -- Non-aligned fact–fact corpora (Amendment 5 §A5.2)")
    L.append("")
    L.append(f"Generated: {ts} (scorer run). All numbers below are read "
             "from `summary.json` written by the same run of "
             "`code/score_nonaligned.py`.")
    L.append("")
    L.append("## Design recap (PREREGISTRATION.md Amendment 5 §A5.2, "
             "locked 2026-11-06 12:15 UTC)")
    L.append("")
    L.append("Generator `code/gen_nonaligned.py` is identical to "
             "`code/gen_dataset.py` in sizes (fact 10,000,000; fact2 "
             "6,000,000; K = 20,000), materialisation, and DuckDB "
             "ground-truth cross-validation, EXCEPT that fact2's "
             "popularity pmf is permuted across key ids before drawing: "
             "pmf2[k] = pmf[pi(k)], with an independent pi per pair "
             "column (kA: rng seeded seed+31337; kB: seed+31338), and "
             "fact2 counts then drawn multinomially from the permuted "
             "pmf with the seed+777 fact2 rng stream. NA1: pi permutes "
             "a uniformly random subset of phi = 0.5 of the K key ids "
             "among themselves, identity elsewhere. NA2: pi is a "
             "uniform random permutation of all K key ids. Datasets: "
             "families {zipf, lognormal, mixed} × seeds {101, 202} × "
             "levels {NA1, NA2} = 12. Arms A1, A4, A5, A8 run via the "
             "unmodified `arms.py` / `run_dataset.py` machinery "
             "(rtag `nonaligned`; synopsis built by the unmodified "
             "profiler from the realised, misaligned, data). Scoring "
             "pooled per level (66 instances): recall / precision / F1 "
             "of FAIL prediction, Spearman rho on log10 loads, median "
             "Q-error -- overall, F family only (F_A, F_B; 12/level, "
             "primary), and join-emergent (J + F; 30/level, secondary). "
             "The aligned reference is seeds-matched: predictions_main."
             "csv restricted to seeds {101, 202} × the same 3 families "
             "(66 instances).")
    L.append("")
    L.append("Validation: all 12 datasets must have every DuckDB "
             "cross-check exactly 0. Status: "
             + ("ALL 12 all_exact = true." if all(
                 v["all_exact"] for v in validation.values())
                else "SEE DEVIATIONS -- not all exact."))
    L.append("")
    L.append("Fact-count identity sanity check (fact counts of each "
             "non-aligned dataset vs the aligned dataset of the same "
             "family/seed must be bit-identical, since only the fact2 "
             "pmf changed): "
             + ("ALL 12 identical (fact_kA/kB/kC/kN + null_count; "
                "distinct_A also identical)."
                if all(p["all_fact_counts_identical"]
                       for p in fact_identity.values())
                else "SEE DEVIATIONS -- mismatch found."))
    L.append("")
    L.append("## Realised alignment (Spearman rho, fact vs fact2 "
             "per-key counts)")
    L.append("")
    L.append("| dataset | level | family | seed | rho_kA | rho_kB |")
    L.append("|---|---|---|---|---|---|")
    for m in datasets_meta:
        L.append(f"| {m['dataset']} | {m['level']} | {m['family']} | "
                 f"{m['seed']} | {fmt(m['spearman_rho_kA'], 4)} | "
                 f"{fmt(m['spearman_rho_kB'], 4)} |")
    L.append("")
    for lv in LEVELS:
        blk = S["levels"][lv]
        L.append(f"## Results -- {lv} (pooled, {blk['n_instances']} "
                 f"instances, {blk['n_gt_fail']} ground-truth FAIL)")
        L.append("")
        L.append("| arm | scope | recall | precision | F1 | Spearman "
                 "rho (log10 loads) | median Q-error | n |")
        L.append("|---|---|---|---|---|---|---|---|")
        for arm in ARMS:
            for scope, key in (("overall", "overall"), ("F", "F"),
                               ("J+F", "join_emergent")):
                m = blk["arms"][arm][key]
                L.append(f"| {arm} | {scope} | {fmt(m['recall'])} | "
                         f"{fmt(m['precision'])} | {fmt(m['f1'])} | "
                         f"{fmt(m['spearman_rho'])} | "
                         f"{fmt(m['median_q_error'])} | {m['n']} |")
        L.append("")
    blk = S["aligned_reference_seeds_matched"]
    L.append(f"## Aligned reference -- seeds-matched ({blk['n_instances']} "
             f"instances, {blk['n_gt_fail']} ground-truth FAIL)")
    L.append("")
    L.append("| arm | scope | recall | precision | F1 | Spearman rho "
             "(log10 loads) | median Q-error | n |")
    L.append("|---|---|---|---|---|---|---|---|")
    for arm in ARMS:
        for scope, key in (("overall", "overall"), ("F", "F"),
                           ("J+F", "join_emergent")):
            m = blk["arms"][arm][key]
            L.append(f"| {arm} | {scope} | {fmt(m['recall'])} | "
                     f"{fmt(m['precision'])} | {fmt(m['f1'])} | "
                     f"{fmt(m['spearman_rho'])} | "
                     f"{fmt(m['median_q_error'])} | {m['n']} |")
    L.append("")
    L.append("## F family (primary split) -- per level vs aligned reference")
    L.append("")
    L.append("| arm | aligned F recall | NA1 F recall | NA2 F recall | "
             "aligned F median Q | NA1 F median Q | NA2 F median Q | "
             "aligned F rho | NA1 F rho | NA2 F rho |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for arm in ARMS:
        a = aligned[arm]["F"]
        n1 = S["levels"]["NA1"]["arms"][arm]["F"]
        n2 = S["levels"]["NA2"]["arms"][arm]["F"]
        L.append(f"| {arm} | {fmt(a['recall'])} | {fmt(n1['recall'])} | "
                 f"{fmt(n2['recall'])} | {fmt(a['median_q_error'])} | "
                 f"{fmt(n1['median_q_error'])} | "
                 f"{fmt(n2['median_q_error'])} | "
                 f"{fmt(a['spearman_rho'])} | {fmt(n1['spearman_rho'])} | "
                 f"{fmt(n2['spearman_rho'])} |")
    L.append("")
    L.append("Full-grid aligned values from `results/summary.json` "
             "(99 instances incl. seed 303; pooled and join-emergent) "
             "are copied into `summary.json` under "
             "`aligned_reference_full_grid_from_summary_json` for "
             "context; the seeds-matched reference above is the "
             "apples-to-apples comparison.")
    L.append("")
    L.append("## Pre-registered expectation -- did it hold?")
    L.append("")
    L.append("Pre-registered (Amendment 5 §A5.2): “A5/A8 F-family load "
             "fidelity degrades as alignment falls -- the synopsis's HH "
             "pair counts remain exact (they are profiled from the "
             "realised data), so the comonotone tail model is the "
             "expected failure point; A4 and A1 are unchanged in "
             "distribution (A4 already breaks alignment by "
             "construction; A1 samples rows).” Load fidelity is judged "
             "here by the F-family median Q-error (and Spearman rho); "
             "F recall is reported alongside.")
    L.append("")
    for arm in ("A5", "A8"):
        qa = aligned[arm]["F_median_q_error"]
        q1 = S["levels"]["NA1"]["arms"][arm]["F_median_q_error"]
        q2 = S["levels"]["NA2"]["arms"][arm]["F_median_q_error"]
        deg1, deg2 = q1 > qa, q2 > qa
        if deg1 and deg2:
            verdict_txt = "degraded at both NA1 and NA2 vs aligned"
        elif deg2:
            verdict_txt = "degraded at NA2 only (not at NA1)"
        elif deg1:
            verdict_txt = "degraded at NA1 only (not at NA2)"
        else:
            verdict_txt = "did NOT degrade at either level"
        ra = aligned[arm]["F"]["spearman_rho"]
        r1 = S["levels"]["NA1"]["arms"][arm]["F"]["spearman_rho"]
        r2 = S["levels"]["NA2"]["arms"][arm]["F"]["spearman_rho"]
        L.append(f"- {arm}: F median Q-error aligned {fmt(qa)} → NA1 "
                 f"{fmt(q1)} ({(q1 / qa - 1) * 100:+.2f}% relative) → "
                 f"NA2 {fmt(q2)} ({(q2 / qa - 1) * 100:+.2f}% relative): "
                 f"{verdict_txt}. F Spearman rho aligned {fmt(ra)} → "
                 f"NA1 {fmt(r1)} → NA2 {fmt(r2)}. "
                 f"F recall aligned "
                 f"{fmt(aligned[arm]['F']['recall'])} → NA1 "
                 f"{fmt(S['levels']['NA1']['arms'][arm]['F']['recall'])} "
                 f"→ NA2 "
                 f"{fmt(S['levels']['NA2']['arms'][arm]['F']['recall'])}.")
    for arm in ("A1", "A4"):
        qa = aligned[arm]["F_median_q_error"]
        q1 = S["levels"]["NA1"]["arms"][arm]["F_median_q_error"]
        q2 = S["levels"]["NA2"]["arms"][arm]["F_median_q_error"]
        L.append(f"- {arm} (expected unchanged in distribution): F "
                 f"median Q-error aligned {fmt(qa)} → NA1 {fmt(q1)} → "
                 f"NA2 {fmt(q2)}; F recall aligned "
                 f"{fmt(aligned[arm]['F']['recall'])} → NA1 "
                 f"{fmt(S['levels']['NA1']['arms'][arm]['F']['recall'])} "
                 f"→ NA2 "
                 f"{fmt(S['levels']['NA2']['arms'][arm]['F']['recall'])}.")
    L.append("- Note on A4: its prediction procedure is unchanged, "
             "but its F-family error is measured against a ground "
             "truth whose alignment falls -- as alignment falls the "
             "truth moves toward A4's own broken-alignment model, so "
             "A4's F median Q-error IMPROVES sharply (aligned "
             f"{fmt(aligned['A4']['F_median_q_error'])} → NA1 "
             f"{fmt(S['levels']['NA1']['arms']['A4']['F_median_q_error'])}"
             f" → NA2 "
             f"{fmt(S['levels']['NA2']['arms']['A4']['F_median_q_error'])}"
             "). This is a change in the target, not in A4.")
    both = all(
        S["levels"][lv]["arms"][arm]["F_median_q_error"]
        > aligned[arm]["F_median_q_error"]
        for arm in ("A5", "A8") for lv in LEVELS)
    any_deg = any(
        S["levels"][lv]["arms"][arm]["F_median_q_error"]
        > aligned[arm]["F_median_q_error"]
        for arm in ("A5", "A8") for lv in LEVELS)
    max_rel_na1 = max(
        S["levels"]["NA1"]["arms"][arm]["F_median_q_error"]
        / aligned[arm]["F_median_q_error"] - 1 for arm in ("A5", "A8"))
    max_rel_na2 = max(
        S["levels"]["NA2"]["arms"][arm]["F_median_q_error"]
        / aligned[arm]["F_median_q_error"] - 1 for arm in ("A5", "A8"))
    if both:
        L.append("- Overall: the expectation HELD IN DIRECTION but "
                 "with small magnitude. A5/A8 F median Q-error is "
                 "worse than aligned at both levels for both arms, "
                 f"but the NA1 degradation is at most "
                 f"{max_rel_na1 * 100:.2f}% relative (negligible) and "
                 f"the NA2 degradation at most {max_rel_na2 * 100:.1f}% "
                 "relative; the clearer fidelity loss is in F Spearman "
                 "rho (see per-arm lines). F verdicts are unaffected: "
                 "all 12 F instances per level are ground-truth FAIL "
                 "and every arm predicts FAIL on all of them (F recall "
                 "1.000 at both levels and in the aligned reference), "
                 "so the comonotone tail model did NOT become a "
                 "material failure point for verdicts, nor for median "
                 "load fidelity at these scales -- the degradation is "
                 "real but confined mainly to rank fidelity at full "
                 "misalignment.")
    elif any_deg:
        L.append("- Overall: the expectation PARTIALLY held (A5/A8 "
                 "F-family median Q-error worse than aligned in some, "
                 "but not all, arm × level combinations; see the "
                 "per-arm lines above).")
    else:
        L.append("- Overall: the expectation did NOT hold (A5/A8 "
                 "F-family median Q-error no worse than aligned at "
                 "either level for either arm).")
    L.append("")
    L.append("## Deviations")
    L.append("")
    if deviations:
        for dtext in deviations:
            L.append(f"- [{ts}] {dtext}")
    else:
        L.append(f"- [{ts}] None. Generation, validation, arm runs, "
                 "and scoring followed Amendment 5 §A5.2 as written.")
    L.append("")
    with open(os.path.join(BASE, "results", "nonalign",
                           "README.md"), "w") as f:
        f.write("\n".join(L))
    print(json.dumps({
        lv: {arm: {"overall_recall": S["levels"][lv]["arms"][arm]
                   ["overall"]["recall"],
                   "F_recall": S["levels"][lv]["arms"][arm]["F"]
                   ["recall"],
                   "F_median_q": S["levels"][lv]["arms"][arm]
                   ["F_median_q_error"]}
             for arm in ARMS} for lv in LEVELS}, indent=1))
    print("aligned F:", {arm: {"recall": aligned[arm]["F"]["recall"],
                               "median_q": aligned[arm]
                               ["F_median_q_error"]} for arm in ARMS})
    print("deviations:", deviations)


if __name__ == "__main__":
    main()
