# tau* held-out 검증 프로토콜 (사전 등록)

작성: 2026-09-11 18:1x KST — **캘리브레이션 결과를 보기 전에 작성됨**
목적: 모델이 예측하는 latency-optimal checkpoint interval의 "위치"를 독립 검증

## 1. 설계 순서 (되돌리지 않음)

1. Calibration 10런 (conf/cal_tstar.json) — 2노드 + 원격 MinIO, 100k키
   - 무장애 6런(tau 4/6/9/14/20/30s): steady_lat vs 1/tau -> delta, l0
   - 장애 4런(tau 8,8,14,14s, 각 3회): D_eff, mu_d (에피소드 12개)
2. delta, l0, D, mu_d, rho **동결**. 이 시점 값을 아래 4절에 기록.
3. tau*_pred 계산 -> validation grid 8점 확정 (아래 3절 규칙)
4. Validation 실행. **검증 런으로는 어떤 파라미터도 재추정하지 않음.**

## 2. Validation 런 사양

- 1200 s/run, warmup 90 s, 런당 **정확히 4회** 장애
- failure exposure rate nu = 4/1200 = **1/300 s^-1**
  (= equivalent failure interval M = 300 s. "매 300초마다"가 아님)
- 장애 시각은 [120, 1060] s 구간의 연속 균등분포에서 추출, 최소 간격 180 s.
  체크포인트 주기(<= 수십 초)와 의도적 phase lock 없음.
- 각 tau 3회 반복, 실행 순서 randomize (저장소 열화가 tau와 상관되지 않도록)

## 3. Grid 규칙 (결과와 무관하게 미리 고정)

tau*_pred 를 얻은 뒤, 8점 grid는 반드시 다음을 포함한다:
- 0.6 x tau*_pred 부근
- tau*_pred 부근 (가장 촘촘하게)
- 1.8 x tau*_pred 부근
나머지 점은 0.45 ~ 2.5 x tau*_pred 범위를 로그 간격으로 채운다.

## 4. 동결 파라미터 (캘리브레이션 후 기입)

delta = (미정)
l0    = (미정)
D     = (미정)
mu_d  = (미정)
rho   = (미정)
tau*_pred = (미정)
grid  = (미정)

## 5. 성공 기준 (사전 정의)

- **최적점 판정 방법**: 각 tau에서 3런의 record-weighted mean latency의
  **중앙값**을 그 tau의 대표값으로 쓴다. measured optimum은 **사전에 정한 8개
  grid point 중 대표값이 최소인 점**으로 정의한다.
  곡선을 새로 적합해 최소점을 구하지 않는다(모델을 하나 더 끼워 넣게 되므로).
- **주 기준(primary)**: |tau*_meas - tau*_pred| / tau*_pred <= 0.25
- **보조 기준(secondary)**: L(tau*) < L(0.6 tau*) 그리고 L(tau*) < L(1.8 tau*)
  (양쪽에서 다시 증가하는 U자 확인)
- **보너스**: 동결된 파라미터로 그린 L-bar(tau) 곡선과 실측 곡선의 비교.
  절대 수준은 l0/delta/D/mu_d가 모두 맞아야 하므로 주 기준으로 쓰지 않는다.

## 6. 사후 확인 (검증 후)

- 각 장애의 a/tau 분포를 확인해 특정 위상에 몰리지 않는지 본다
  (a ~ U(0,tau) 가정에 대한 방어 자료)
- 런 순서 대 체크포인트 소요 상관을 확인해 저장소 열화 여부를 점검한다
- 같은 tau의 3런 간 산포를 보고한다

## 7. 용어 주의

- tau < 2d 는 **Flink의 물리적 하한이 아니다.** 정상상태 1/tau 회귀를 안정적으로
  측정하기 위한 **경험적 분석 임계값**이다. "physical floor", "minimum possible
  interval" 로 쓰지 않는다.
- M = 300 s 선택 이유: 예측 최적점을 위 경험적 사용가능 임계값 위로 올리기 위해서.
  결과를 보고 고른 값이 아니라 설계 단계에서 정함.
