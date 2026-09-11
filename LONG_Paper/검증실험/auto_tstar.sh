#!/bin/bash
# 보정 완료 -> 파라미터 동결 -> 검증 설정 생성 -> 검증 기동. 전부 서버에서 자립.
# 외부 접속이 끊겨도 진행되며, 각 단계에서 휴대폰 알림을 보낸다.
cd /home/wontak/icngc2_stream
TOPIC=ICNGC2026LWT
say() { # 제목 태그 본문  (실패해도 실험은 계속)
  for i in 1 2 3; do
    curl -s -m 15 -H "Title: $1" -H "Tags: $2" -d "$3" "https://ntfy.sh/$TOPIC" >/dev/null && return 0
    sleep 20
  done
}
log() { echo "$(date +%F\ %T) $*" >> logs/auto_tstar.log; }

while ! grep -q "sweep done" logs/cal_tstar.log 2>/dev/null; do sleep 60; done
log "보정 완료 감지"

python3 freeze.py > logs/freeze.out 2>&1
if [ ! -f frozen_tstar.json ]; then
  say "[경고] 동결 실패" rotating_light "freeze.py 실패. logs/freeze.out 확인 필요. 검증 미기동."
  log "freeze 실패"; exit 1
fi

read TS TH RATIO GRID <<< $(python3 -c "
import json; f=json.load(open(\"frozen_tstar.json\"))
print(f[\"tau_star_pred\"], f[\"analysis_threshold_2d\"], f[\"tau_star_pred\"]/f[\"analysis_threshold_2d\"],
      \",\".join(str(g) for g in f[\"grid_s\"]))")
log "tau*=$TS 2d=$TH ratio=$RATIO grid=$GRID"

# 예측 최적점이 분석 임계값에 너무 가까우면 그대로 돌려봐야 판별이 안 된다 -> 사람 판단 요청
OK=$(python3 -c "print(1 if $RATIO >= 1.5 else 0)")
if [ "$OK" != "1" ]; then
  say "[판단 필요] tau*가 임계값에 근접" warning \
    "tau*_pred=${TS}s, 분석임계 2d=${TH}s, 비율 ${RATIO}. 1.5 미만이라 양쪽 가지 확인 불가. M을 600s로 올릴지 결정 필요. 검증 미기동."
  log "ratio<1.5 -> 대기"; exit 0
fi

python3 - <<PY
import json, random
f=json.load(open("frozen_tstar.json")); rng=random.Random(20260911)
MEAS,NF,WARM=1200,4,90
def offs():
    while True:
        o=sorted(rng.uniform(120,MEAS-140) for _ in range(NF))
        if all(b-a>=180 for a,b in zip(o,o[1:])): return [round(x,1) for x in o]
c=[]
for rep in range(3):
    for t in f["grid_s"]:
        c.append({"run_id":f"val_k100k_t{str(t).replace(\".\",\"p\")}_r{rep}",
                  "rate":58277,"keys":100000,"payload":1024,"spin":32000,
                  "ckpt_ms":int(round(t*1000)),"warmup_s":WARM,"measure_s":MEAS,
                  "failures":offs(),"parts":8})
rng.shuffle(c)
json.dump(c,open("conf/val_tstar.json","w"),indent=1)
print(len(c))
PY

pkill -f "java.*EventGen" 2>/dev/null
python3 -c "import sys;sys.path.insert(0,\".\");import driver_2node as D;D.stop_gen();D.cancel_all();D.ensure_tms(per_node=2)"
rm -f logs/val_tstar.log logs/val_tstar.log.done
setsid nohup python3 sweep_2node.py conf/val_tstar.json logs/val_tstar.log > logs/val_tstar.out 2>&1 < /dev/null &
log "검증 기동 pid=$!"
say "파라미터 동결 완료" lock "tau*_pred=${TS}s (임계 ${TH}s, 비율 ${RATIO}). 검증 24런 기동. 약 9시간."

# 검증 완료 감시
while ! grep -q "sweep done" logs/val_tstar.log 2>/dev/null; do sleep 120; done
N=$(grep -c "    ok " logs/val_tstar.log)
say "검증 완료" tada "held-out 검증 ${N}/24런 종료. 판정 대기."
log "검증 완료 $N런"
