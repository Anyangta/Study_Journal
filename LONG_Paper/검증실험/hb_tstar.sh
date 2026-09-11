#!/bin/bash
# tau* 검증 캠페인 1시간 하트비트. 진행상황 + 이상 감지.
cd /home/wontak/icngc2_stream
TOPIC=ICNGC2026LWT
while true; do
  sleep 3600
  # 어느 단계인가
  if [ -f logs/val_tstar.log ]; then STAGE=검증; LOG=logs/val_tstar.log; TOT=24
  else STAGE=보정; LOG=logs/cal_tstar.log; TOT=10; fi
  N=$(grep -c "    ok " $LOG 2>/dev/null || echo 0)
  ALIVE=$(pgrep -c -f "[s]weep_2node" || echo 0)
  # 최근 체크포인트 소요(저장소 열화 감시)
  CK=$(for d in $(ls -dt results/*/ 2>/dev/null | head -3); do
         python3 -c "
import json,os,sys
try:
  c=[x for x in json.load(open(os.path.join(\"$d\",\"ckpts.json\"))) if x.get(\"status\")==\"COMPLETED\"]
  if c: print(\"%.1f\"%(sorted(x[\"end_to_end_duration\"] for x in c)[len(c)//2]/1000))
except: pass" 2>/dev/null; done | tr "\n" "/")
  if [ "$ALIVE" -eq 0 ] && ! grep -q "sweep done" $LOG 2>/dev/null; then
    curl -s -H "Title: [경고] 실험 멈춤" -H "Priority: urgent" -H "Tags: rotating_light" \
      -d "$STAGE 단계 $N/$TOT 에서 스윕 프로세스가 없음. 확인 필요." "https://ntfy.sh/$TOPIC" >/dev/null
  else
    ETA=$(python3 -c "
import sys
n=$N; tot=$TOT; per=(1345 if \"$STAGE\"==\"검증\" else 500)
left=(tot-n)*per/3600
print(\"%.1f시간\"%left if left>0 else \"곧\")" 2>/dev/null)
    curl -s -H "Title: ICNGC 실험 진행중" -H "Tags: hourglass" \
      -d "$STAGE $N/$TOT 완료. 남은시간 약 $ETA. 체크포인트소요 ${CK}초(열화감시)." \
      "https://ntfy.sh/$TOPIC" >/dev/null
  fi
  echo "$(date +%F\ %T) hb: $STAGE $N/$TOT alive=$ALIVE ck=$CK" >> logs/hb_tstar.log
done
