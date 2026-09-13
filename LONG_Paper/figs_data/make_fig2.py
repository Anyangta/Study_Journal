#!/usr/bin/env python3
"""
Figure 2 (복구 궤적) 를 fig2_data.csv 에서 직접 그립니다.

실행:
    pip install matplotlib          # 처음 한 번만
    python make_fig2.py

결과: fig2_mine.pdf, fig2_mine.png  (같은 폴더)

데이터 출처: m9_p1024_k200k_s32000_r30_c80_m55_0 의 4번 장애 에피소드.
  - 200k key, rho = 0.30, tau = 89.1 s
  - 서버의 results/<run_id>/lat.csv 를 1초 구간으로 묶은 것
  - 자세한 상수는 fig2_data_meta.json
"""
import csv, json, os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))

# ── 데이터 ────────────────────────────────────────────────────────────────
t, lat, thr = [], [], []
with open(os.path.join(HERE, "fig2_data.csv"), encoding="utf-8") as f:
    for row in csv.DictReader(f):
        t.append(float(row["time_s"]))
        lat.append(float(row["latency_s"]))
        thr.append(float(row["throughput_krec_s"]))

M = json.load(open(os.path.join(HERE, "fig2_data_meta.json"), encoding="utf-8"))
STEADY   = M["steady_latency_s"]      # 0.2276 s  — 정상 지연
LAMBDA   = M["lambda_krec_s"]         # 32.677 k rec/s — 입력률
RESTORED = M["restored_at_s"]         # 1.077 s   — 엔진이 복원을 마친 시각

# ── 스타일: IEEE 2단 1칼럼 폭(3.4 in), 본문보다 작지 않은 글씨 ──────────────
plt.rcParams.update({
    "font.size": 8, "axes.labelsize": 8,
    "xtick.labelsize": 7, "ytick.labelsize": 7,
    "axes.linewidth": 0.6, "lines.linewidth": 1.4,
    "figure.dpi": 150, "savefig.dpi": 300, "savefig.bbox": "tight",
    "axes.spines.top": False, "axes.spines.right": False,
})
BLUE, RED, GREEN, ORANGE, GREY = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#52514e"

fig, (ax, ax2) = plt.subplots(
    2, 1, figsize=(3.4, 3.2), sharex=True, gridspec_kw={"height_ratios": [2, 1]})

# ── 위 패널: 지연 ─────────────────────────────────────────────────────────
ax.plot(t, lat, color=BLUE, lw=1.6)
ax.axhline(STEADY, color=GREY, lw=0.8, dashes=(3, 2))          # 정상 수준
ax.annotate("steady", xy=(t[-1], STEADY), xytext=(-2, 3),
            textcoords="offset points", ha="right", color=GREY, fontsize=6.5)
ax.axvline(0, color=RED, lw=1.0)                                # 장애 주입
ax.annotate("kill", xy=(0, ax.get_ylim()[1]), xytext=(-3, -9),
            textcoords="offset points", color=RED, fontsize=6.5, ha="right")
ax.axvline(RESTORED, color=GREEN, lw=1.0, dashes=(4, 2))        # 복원 완료
ax.annotate("restored", xy=(RESTORED, ax.get_ylim()[1]), xytext=(2, -18),
            textcoords="offset points", color=GREEN, fontsize=6.5)
ax.set_ylabel("latency (s)")
ax.grid(axis="y", color="#d8d7d2", lw=0.5)
ax.set_axisbelow(True)

# ── 아래 패널: 처리량 ─────────────────────────────────────────────────────
# 여기가 메커니즘입니다. 복구 중 처리량이 lambda 의 약 3배로 올라가 backlog 를
# 빼고, 다 빠지면 다시 lambda 로 돌아옵니다. 즉 backlog 는 mu_d - lambda 로 줄어듭니다.
ax2.plot(t, thr, color=ORANGE, lw=1.4)
ax2.axhline(LAMBDA, color=GREY, lw=0.8, dashes=(3, 2))
ax2.annotate(r"$\lambda$", xy=(t[-1], LAMBDA), xytext=(-2, 3),
             textcoords="offset points", ha="right", color=GREY, fontsize=6.5)
ax2.axvline(0, color=RED, lw=1.0)
ax2.set_ylabel("throughput\n(k rec/s)")
ax2.set_xlabel("time relative to failure (s)")
ax2.grid(axis="y", color="#d8d7d2", lw=0.5)
ax2.set_axisbelow(True)

fig.tight_layout(h_pad=0.4)
for ext in ("pdf", "png"):
    out = os.path.join(HERE, "fig2_mine." + ext)
    fig.savefig(out)
    print("wrote", out)
