#!/usr/bin/env python3
"""
Figure 4 (τ* vs M scaling) 재생성.

기존 fig2_scaling.png에는 "measured argmin" 산점이 함께 찍혀 있었으나, 그 점들은
상태 크기·저장소 조건이 서로 다른 run에서 얻은 것이라 δ·D가 제각각이다. 반면 곡선은
기준 arm 하나(δ=1.907 s, D=4.28 s, ρ=0.50)로 그린 것이므로 두 가지를 같은 축에 겹치면
"모델이 실측을 못 맞힌다"로 오독된다. 이 그림의 주장은 값의 일치가 아니라 **두 목적함수의
증가율(스케일링 지수) 차이**이므로 곡선만 남긴다.

파라미터는 전부 논문 본문에 적힌 값에서 온 것이라 데이터 파일이 필요 없다.
"""
import math
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ---- 논문 본문의 기준 arm 값 -------------------------------------------------
DELTA = 1.907      # 지연 등가 체크포인트 비용 (s), Table III 200MB·빠름
D = 4.28           # 실효 공백 (s)
RHO = 0.50         # 가동률
RHO_D = 0.5563     # λ/μ_d — 표 4의 τ* 값과 일치하도록 역산된 값
DELTA_CAP = 0.22   # 용량 기준 체크포인트 비용 (s), 표 3 상한

C = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#4a3aa7"]
INK, INK2, GRID = "#0b0b0b", "#52514e", "#d8d7d2"

plt.rcParams.update({
    "font.size": 8, "axes.labelsize": 8, "axes.titlesize": 8,
    "xtick.labelsize": 7, "ytick.labelsize": 7, "legend.fontsize": 7,
    "axes.edgecolor": INK2, "axes.linewidth": 0.6,
    "xtick.color": INK2, "ytick.color": INK2,
    "axes.labelcolor": INK, "text.color": INK,
    "grid.color": GRID, "grid.linewidth": 0.5,
    "figure.dpi": 150, "savefig.dpi": 300, "savefig.bbox": "tight",
    "legend.frameon": False, "axes.spines.top": False, "axes.spines.right": False,
    "font.family": "DejaVu Sans",
})


def tau_star_latency(M):
    """δ²·M·(1−ρ_d)/(1−ρ) = τ²·(2τ/3 + D) 를 τ에 대해 푼다."""
    k = (1.0 - RHO_D) / (1.0 - RHO)
    rhs = k * DELTA * DELTA * M
    f = lambda t: t * t * (2.0 * t / 3.0 + D) - rhs
    lo, hi = 1e-4, 1.0
    while f(hi) < 0 and hi < 1e7:
        hi *= 2.0
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        if f(mid) < 0:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def tau_star_capacity(M):
    """용량/낭비 기준 baseline: √(2·δ_cap·M/ρ)"""
    return math.sqrt(2.0 * DELTA_CAP * M / RHO)


def tau_stable_max(M):
    """평균 부하 안정 조건 λ(τ/2+D)/(μ_d−λ) + D < M 의 경계."""
    r = RHO_D / (1.0 - RHO_D)          # λ/(μ_d−λ)
    return max(0.0, 2.0 * ((M - D) / r - D))


def main():
    Ms = np.logspace(np.log10(20), np.log10(2e5), 400)
    lat = np.array([tau_star_latency(M) for M in Ms])
    cap = np.array([tau_star_capacity(M) for M in Ms])
    stab = np.array([tau_stable_max(M) for M in Ms])

    fig, ax = plt.subplots(figsize=(3.4, 2.6))

    ax.fill_between(Ms, stab, 1e5, color=C[4], alpha=0.10, lw=0, zorder=0)
    ax.plot(Ms, stab, color=C[4], lw=1.1, dashes=(5, 2, 1, 2), zorder=2,
            label="mean-load stability bound")
    ax.plot(Ms, cap, color=C[1], lw=1.9, dashes=(5, 2), zorder=3,
            label="capacity / wasted work  ($M^{1/2}$)")
    ax.plot(Ms, lat, color=C[0], lw=1.9, zorder=4,
            label="latency  (0.43 $\\rightarrow$ 1/3)")

    # 두 기준의 비가 벌어지는 것을 한 지점에서 직접 보여준다
    Mx = 86400.0
    lo, hi = tau_star_latency(Mx), tau_star_capacity(Mx)
    ax.annotate("", xy=(Mx, hi), xytext=(Mx, lo),
                arrowprops=dict(arrowstyle="<->", color=INK2, lw=0.8))
    ax.annotate(f"×{hi/lo:.1f}", xy=(Mx, math.sqrt(lo * hi)), xytext=(5, 0),
                textcoords="offset points", fontsize=6.5, color=INK, va="center")

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_ylim(1.5, 3e3)
    ax.set_xlabel("mean time between failures $M$ (s)")
    ax.set_ylabel("optimal checkpoint interval $\\tau^*$ (s)")
    ax.grid(True, which="major", alpha=0.7)
    ax.set_axisbelow(True)
    ax.legend(loc="upper left", handlelength=2.2, borderpad=0.2, labelspacing=0.25)

    ax2 = ax.twiny()
    ax2.set_xscale("log")
    ax2.set_xlim(ax.get_xlim())
    ax2.set_xticks([3600, 86400])
    ax2.set_xticklabels(["1 h", "1 d"])
    ax2.minorticks_off()
    ax2.tick_params(axis="x", length=2)
    for sp in ("top", "right", "left", "bottom"):
        ax2.spines[sp].set_visible(False)

    for ext in ("png", "pdf"):
        fig.savefig(f"figs/fig2_scaling.{ext}")
    print("wrote figs/fig2_scaling.png / .pdf")

    print("\n검산 (논문 표 4와 대조):")
    print(f"{'M':>8} {'지연 τ*':>9} {'용량 τ*':>9} {'배율':>6} {'안정한계':>9}")
    for M in (30, 55, 600, 3600, 86400):
        a, b = tau_star_latency(M), tau_star_capacity(M)
        print(f"{M:>8} {a:>9.3f} {b:>9.3f} {b/a:>8.3f} {tau_stable_max(M):>9.1f}")


if __name__ == "__main__":
    main()
