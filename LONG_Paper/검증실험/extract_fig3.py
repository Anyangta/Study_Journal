#!/usr/bin/env python3
"""
Figure 3의 근거 데이터를 에피소드 단위 CSV로 추출한다. 서버에서 실행.

각 행은 장애 에피소드 하나이며, 그 에피소드에서 **직접 측정한** λ, μ_d, 최대 지연으로
계산한 기하식 값과 같은 에피소드의 실측 초과 지연 면적을 함께 담는다.
기하식에는 데이터에 적합시킨 파라미터가 없다.

    A_geo = μ_d · λ · peak² / (2(μ_d − λ))

run 단위 제외 플래그(포화/불안정/과부하)를 열로 남겨, 논문 Figure 3의 모집단과
전체 모집단 어느 쪽 수치도 이 파일 하나로 재현할 수 있게 했다.
"""
import csv
import os
import statistics as st
import sys

ROOT = "/home/wontak/icngc2_stream"
sys.path.insert(0, ROOT)
import analyze  # noqa: E402


def main():
    rows = []
    res = os.path.join(ROOT, "results")
    for d in sorted(os.listdir(res)):
        if not os.path.exists(os.path.join(res, d, "meta.json")):
            continue
        try:
            r = analyze.analyze_run(d)
        except Exception:
            continue
        if not r.get("episodes"):
            continue
        lam = r.get("lambda_meas")
        if not lam:
            continue

        gap = r.get("ckpt_gap_s")
        dur = (r.get("ckpt_dur_ms") or 0) / 1000.0
        saturated = bool(gap and dur > 0 and gap < 2.0 * dur)
        unstable = bool((r.get("steady_bins") or 99) < 12
                        or (r.get("steady_lat_ms") or 0) > 5000)
        overloaded = bool(r.get("overloaded"))

        for e in r["episodes"]:
            if e.get("overlapped") or e.get("truncated"):
                continue
            md = e.get("mu_drain")
            pk = e.get("peak_lat_ms")
            ar = e.get("lag_area_rec_s")
            if not md or not pk or not ar or ar <= 0 or md <= lam:
                continue
            peak_s = pk / 1000.0
            geo = md * lam * peak_s * peak_s / (2.0 * (md - lam))
            rows.append({
                "run_id": d,
                "keys": r["keys"],
                "backend": r["backend"],
                "ckpt_gap_s": round(gap or -1, 2),
                "ckpt_dur_s": round(dur, 2),
                "lambda_meas": round(lam, 1),
                "mu_drain": round(md, 1),
                "ckpt_age_s": round(e.get("ckpt_age_s") or -1, 3),
                "D_eff_s": round(e.get("D_eff_s") or -1, 3),
                "peak_lat_s": round(peak_s, 3),
                "A_geo_rec_s": round(geo, 1),
                "A_measured_rec_s": round(ar, 1),
                "ratio_measured_over_geo": round(ar / geo, 4),
                "run_saturated": int(saturated),
                "run_unstable": int(unstable),
                "run_overloaded": int(overloaded),
                "in_figure3": int(not (saturated or unstable or overloaded)),
            })

    out = os.path.join(ROOT, "fig3_episodes.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    fig = [x for x in rows if x["in_figure3"]]
    print(f"전체 에피소드 {len(rows)}개 -> {out}")
    print(f"Figure 3 모집단 {len(fig)}개 (포화/불안정/과부하 run 제외)")
    print(f"중앙 배율  전체 {st.median(x['ratio_measured_over_geo'] for x in rows):.3f}"
          f"  /  Figure 3 {st.median(x['ratio_measured_over_geo'] for x in fig):.3f}")
    for k in sorted({x["keys"] for x in fig}):
        print(f"  keys={k:>7}: {sum(1 for x in fig if x['keys'] == k)}개")


if __name__ == "__main__":
    main()
