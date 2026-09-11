#!/usr/bin/env python3
"""
캘리브레이션 -> 파라미터 동결 -> tau*_pred -> 검증 격자 생성.

PROTOCOL_tstar.md 의 규칙을 그대로 구현한다.
  - 무장애 런: steady_lat(s) vs 1/tau 선형회귀 -> 기울기 s, 절편 l0
               delta = sqrt(2 * s * (1-rho))
  - 장애 런:   에피소드별 D_eff(= peak - a), mu_drain 의 중앙값
  - tau*:      delta^2 * M * (1-rho_d)/(1-rho) = tau^2 * (2tau/3 + D)
  - 격자:      0.6 tau*, tau* 부근, 1.8 tau* 를 반드시 포함, 0.45~2.5 tau* 로그 간격 8점
서버에서 실행한다(결과 디렉터리 접근 필요).
"""
import json, math, os, statistics as st, sys

ROOT = "/home/wontak/icngc2_stream"
RES = os.path.join(ROOT, "results")
RHO = 0.50          # 설계값 (rate = 58277 = 0.5 * mu)

def jload(p):
    with open(p) as f: return json.load(f)

def lstsq(x, y):
    n=len(x); mx=sum(x)/n; my=sum(y)/n
    sxx=sum((a-mx)**2 for a in x); sxy=sum((a-mx)*(b-my) for a,b in zip(x,y))
    b1=sxy/sxx; b0=my-b1*mx
    pred=[b0+b1*a for a in x]
    ssr=sum((b-p)**2 for b,p in zip(y,pred)); sst=sum((b-my)**2 for b in y)
    return b1, b0, (1-ssr/sst if sst>0 else float("nan"))

def run_summary(rid):
    sys.path.insert(0, ROOT)
    import analyze
    return analyze.analyze_run(rid)

def main():
    nf, fr = [], []
    for d in sorted(os.listdir(RES)):
        if d.startswith("cal_nf_"): nf.append(d)
        elif d.startswith("cal_f_"): fr.append(d)
    print(f"무장애 {len(nf)}런, 장애 {len(fr)}런")

    # --- delta, l0 (무장애 런)
    pts=[]
    for rid in nf:
        r = run_summary(rid)
        g, sl, cd = r.get("ckpt_gap_s"), r.get("steady_lat_ms"), (r.get("ckpt_dur_ms") or 0)/1000
        if not g or not sl: continue
        sat = g < 2.0*cd
        print(f"  {rid:<16} gap={g:6.2f}s  steady={sl:8.1f}ms  ckdur={cd:5.2f}s"
              f"{'  [포화 제외]' if sat else ''}")
        if not sat: pts.append((1.0/g, sl/1000.0))
    if len(pts) < 3: raise SystemExit("무장애 점이 3개 미만 — 동결 불가")
    slope, l0, r2 = lstsq([p[0] for p in pts], [p[1] for p in pts])
    slope = max(slope, 0.0)
    delta = math.sqrt(2*slope*(1-RHO))
    print(f"\ndelta 적합: 기울기={slope:.4f}  l0={l0*1000:.1f}ms  R2={r2:.3f}  -> delta={delta:.3f}s  ({len(pts)}점)")

    # --- D, mu_d (장애 런)
    Ds, Ms_, ages = [], [], []
    for rid in fr:
        r = run_summary(rid)
        for e in r.get("episodes", []):
            if e.get("overlapped") or e.get("truncated"): continue
            if e.get("D_eff_s") is not None: Ds.append(e["D_eff_s"])
            if e.get("mu_drain"): Ms_.append(e["mu_drain"])
            if e.get("ckpt_age_s") is not None: ages.append(e["ckpt_age_s"])
    if not Ds or not Ms_: raise SystemExit("장애 에피소드 부족 — 동결 불가")
    D  = st.median(Ds); mu_d = st.median(Ms_)
    lam = 58277.0; rho_d = lam/mu_d
    print(f"D_eff 중앙값={D:.2f}s ({len(Ds)}에피소드)   mu_drain 중앙값={mu_d:.0f}/s  rho_d={rho_d:.3f}")

    # --- tau*
    def tau_star(M):
        k=(1-rho_d)/(1-RHO); rhs=k*delta*delta*M
        f=lambda t: t*t*(2*t/3+D)-rhs
        lo,hi=1e-4,1.0
        while f(hi)<0 and hi<1e7: hi*=2
        for _ in range(200):
            m=(lo+hi)/2
            if f(m)<0: lo=m
            else: hi=m
        return (lo+hi)/2
    M=300.0; ts=tau_star(M)
    ckdur=st.median([ (run_summary(r).get("ckpt_dur_ms") or 0)/1000 for r in nf ])
    print(f"\n>>> tau*_pred (M={M:.0f}s) = {ts:.2f}s   |  분석 임계값 2d = {2*ckdur:.2f}s"
          f"   비율 tau*/2d = {ts/(2*ckdur):.2f}")

    # --- 격자 (프로토콜 규칙)
    anchors=[0.6*ts, ts, 1.8*ts]
    lo_, hi_ = 0.45*ts, 2.5*ts
    fill=[lo_*(hi_/lo_)**(i/4) for i in range(5)]
    grid=sorted({max(1.0, round(v,1)) for v in anchors+fill})
    while len(grid)>8:
        gaps=[(grid[i+1]/grid[i], i) for i in range(len(grid)-1)]
        gaps.sort(); rm=gaps[0][1]
        cand=grid[rm] if abs(grid[rm]-ts)>abs(grid[rm+1]-ts) else grid[rm+1]
        if abs(cand-0.6*ts)<1e-9 or abs(cand-ts)<1e-9 or abs(cand-1.8*ts)<1e-9:
            cand=grid[rm+1] if cand==grid[rm] else grid[rm]
        grid.remove(cand)
    print(f"검증 격자 {len(grid)}점: {grid}")
    print(f"  (0.6tau*={0.6*ts:.1f}, tau*={ts:.1f}, 1.8tau*={1.8*ts:.1f} 포함 확인)")

    frozen=dict(rho=RHO, rho_d=rho_d, lam=lam, delta_s=delta, l0_s=l0, D_s=D,
                mu_drain=mu_d, M_s=M, tau_star_pred=ts, ckpt_dur_s=ckdur,
                analysis_threshold_2d=2*ckdur, grid_s=grid,
                delta_fit_r2=r2, delta_fit_points=len(pts), n_episodes=len(Ds),
                ckpt_age_mean=st.mean(ages) if ages else None)
    json.dump(frozen, open(os.path.join(ROOT,"frozen_tstar.json"),"w"), indent=1)
    print(f"\n동결 저장: {ROOT}/frozen_tstar.json")

if __name__ == "__main__":
    main()
