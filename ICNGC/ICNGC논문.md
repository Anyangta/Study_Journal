# ICNGC / ICNGC논문

## 2026-09-05 — 서버환경확인-netem대상지정 (v1.0.1)

### 한 일

- **서버 환경 확인 완료. 우려했던 문제 없음.**
  - cluster03 / cluster04 = **별개의 물리 머신 2대** (machine-id 상이,
    `systemd-detect-virt: none` 베어메탈). 같은 공인 IP에 포트만 다른 거라
    한 머신 위 컨테이너일까 의심했는데 아니었음
  - CPU: Intel i9-9900K, `nproc`=16 (물리 8코어 / 하이퍼스레딩 16스레드)
  - **직결 LAN**: `enp1s0f0`에서 172.29.54.8 ↔ 172.29.54.9 (같은 /24).
    브리지를 안 거치는 실제 물리 NIC라 netem이 정상 동작함
- 역할 배정 확정
  | | 호스트 | 사설 IP | SSH 포트 | 역할 |
  |---|---|---|---|---|
  | 사이트 B | cluster03 | 172.29.54.8 | 5409 | 데이터 클라우드 (MinIO + 실행 에이전트) |
  | 사이트 A | cluster04 | 172.29.54.9 | 5408 | 분석 클라우드 (`runner.py`) |
- **두 대 모두 살아있는 Kubernetes 노드임을 발견** (`flannel.1`, `cni0`, docker
  컨테이너 다수). 그래서 `setup/netem.sh`를 전면 재작성:
  인터페이스 root qdisc 전체에 걸던 방식 → **실험 포트(9000, 8800)로 나가는
  트래픽만 대상 지정**. `prio` qdisc의 priomap을 전부 0으로 둬서 매칭 안 된
  트래픽은 unshaped band(1:1)로 흘리고, u32 필터로 peer IP + sport 매칭된
  것만 band 1:3의 netem으로 보냄
- `config.py`의 `B_HOST`를 사설 IP 172.29.54.8로 변경
- `setup/measure_net.sh`를 PEER 사설 IP 기반으로 갱신 + load average 출력 추가

### 다음 할 일

- 코드 복사 (Git Bash에서, 계정 `wontak`)
  ```
  cd "/c/Users/Administrator/OneDrive/바탕 화면"
  scp -P 5409 -r icngc_pushdown wontak@220.149.241.201:~/
  scp -P 5408 -r icngc_pushdown wontak@220.149.241.201:~/
  ```
- 사이트 B(5409)에서: `sudo -n true` 확인 / `ss -tlnp | grep -E ':(9000|8800|9001)'` /
  `uptime` / `iperf3 -s` 띄우기
- 사이트 A(5408)에서: `PEER=172.29.54.8 ./setup/measure_net.sh`
- **미확인 3가지 (이 값들이 나와야 다음 단계 확정 가능)**
  1. `ethtool` Speed — 1000Mb/s인지 10000Mb/s인지에 따라 실험 대역폭 2조건이 정해짐
  2. load average — k8s 노드 공유 중이라 부하 높으면 측정 노이즈. 시간대 조정 또는 반복 증가
  3. 9000/8800 포트 충돌 여부 — 쓰이는 중이면 `config.py`와 `netem.sh`의 `PORTS` 동시 변경
- **`sudo`가 안 되면 netem을 못 걸어서 대역폭 조건 실험 자체가 불가.
  그 경우 설계 변경 필요** (링크 1조건 고정 + 코어 축만으로 축소)
- 이후 Day 1 잔여(MinIO 기동 → `gen_data.py` → `agent_b.py`) → 09/06 첫 실험 배치

### 참고할 맥락 / 결정 사항

- 접속: `ssh -p 5409 wontak@220.149.241.201` (cluster03=B),
  `ssh -p 5408 wontak@220.149.241.201` (cluster04=A)
- **실험 트래픽은 반드시 사설 IP(172.29.54.x)를 쓸 것.** 공인 IP
  220.149.241.201 경로는 NAT를 왕복하므로 netem 통제 밖으로 벗어남
- **인터페이스 전체 shaping 금지.** 두 대가 공용 k8s 노드라서 root qdisc에
  netem을 걸면 flannel VXLAN, 다른 사람 워크로드, 본인 SSH까지 같이 느려짐.
  반드시 포트 대상 지정 방식(`netem.sh apply`)을 쓸 것
- `nproc`=16은 하이퍼스레딩 포함 값. 물리 코어는 8개. 실험의 "B 가용 코어"
  축(1/2/4/8)은 DuckDB `SET threads`로 조절하므로 그대로 유효
- 실제 코드는 이 저널에 커밋하지 않음 (저장소 규칙).
  위치: `바탕 화면/icngc_pushdown/`

## 2026-09-05 — 논문주제확정-교차클라우드복제시점 (v1.0.0)

### 한 일

- 논문 주제 확정: **교차 클라우드에서 같은 데이터를 반복 조회할 때, 몇 번째 질의부터
  원격 조회를 멈추고 데이터를 복제해야 하는가(k\* 결정)**.
  제목 후보 — *How Many Queries Before You Move the Data? Latency- and
  Egress-Aware Replication for Cross-Cloud Analytics*
- 후보 A~F 검토 후 D(연산 푸시다운) 선택 → **측정으로 전제가 반증되어** 반복 질의 k 축으로 피벗.
  (자세한 반증 근거는 아래 "결정 사항" 참고)
- 실험 하네스 전체 작성 완료. 위치: `바탕 화면/icngc_pushdown/` (저널 저장소 밖, 커밋 안 함)
  - `agent_b.py`(B의 SQL 실행 에이전트), `gen_data.py`, `planner.py`(footer 기반 추정+비용모델),
    `runner.py`, `run_all.sh`, `plot.py`, `dry_run.py`, `setup/{measure_net,netem,start_minio}.sh`
- 서버 반입 전 오프라인 검증 통과
  - zone-map 선택도 추정기: σ ∈ [0.001, 1.0]에서 최대 절대오차 **0.0008**
  - `plot.py` 전 구간 리허설: 예측 k\* 대 측정 k\* 중앙 상대오차 **2.9%**, 그림 3장 렌더링 확인
- 서버에 올리기 전에 버그 4개 수정
  1. 전송 바이트를 Parquet의 압축 **해제** 크기로 계산 (→ `total_compressed_size`로 수정)
  2. 캘리브레이션이 `SELECT count(*)` — DuckDB가 footer만 읽어 스캔 속도가 측정 안 됨
  3. `runner.py` 재작성이 조용히 실패해 옛 버전이 남아 있던 것
  4. `plot.py`의 `plt.close` 가 주석 처리된 것

### 다음 할 일

- **[미해결, 최우선] 두 서버 간 물리 링크 속도 미확인.** A에서 `setup/measure_net.sh <B_IP>`
  실행 (B에서 `iperf3 -s` 먼저). 이 값으로 목표 대역폭 2조건 확정:
  물리 10G → `{1gbit, 100mbit}`, 물리 1G → `{500mbit, 100mbit}`
- 09/05: B에 MinIO 기동 → `gen_data.py`(파일 ≥ 1 GiB 확인) → `agent_b.py` 상주
- 09/06: `NET_LABEL=1gbit_50ms ./run_all.sh` (~20분).
  fig1 교차점이 생기는지, fig2의 두 곡선이 저선택도에서 갈라지는지 확인
- 09/07: `NET_LABEL=100mbit_50ms ./run_all.sh` (~45분), 여유 있으면 TPC-H 추가
- **09/08: 실험 종료 마감** → 09/11~12 초고, 09/13 완성고, 09/14 포맷/교정, **09/15 투고**
- Intro/Related Work는 실험이 도는 09/06~07에 미리 써둘 것

### 참고할 맥락 / 결정 사항

- **제약**: ICNGC short paper 4장 이내, 마감 09/15, 실험은 09/08까지. 8코어 서버 2대,
  클라우드 미사용, 무료 플랫폼만, 도커 없음(→ MinIO 단일 바이너리로 해결)
- **테스트베드**: A=분석 클라우드, B=데이터 클라우드(MinIO + 실행 에이전트).
  교차 클라우드 WAN은 B의 egress에 `tc netem`으로 에뮬레이션(root 필요).
  egress 요금은 실측이 아니라 공개 단가표(AWS 0.09 USD/GB 등)로 **모델링**.
  논문에는 "WAN-emulated cross-cloud testbed"로 정직하게 기술할 것
- **원래 주제가 깨진 이유 (논문 도입부의 근거로 그대로 쓸 것)**:
  단일 질의에서는 푸시다운이 **어떤 조건에서도** 이긴다. 1스레드 스캔 속도가
  단순합계 1034 / `LIKE` 695 / `regexp_matches` 511 / 정규식+upper 336 MB/s인데,
  WAN은 100Mbit=12.5, 1Gbit=125 MB/s. CPU가 항상 훨씬 빠르므로 교차점이 없다.
  B의 코어를 1개로 줄여도 마찬가지 (0.057s vs 0.832s)
- **그래서 k(반복 질의 횟수) 축으로 전환**. 세 경로:
  `FULL_PUSH`(매 질의마다 egress) / `PROJECT_ONLY`(투영 컬럼만 1회 복제) /
  `FULL_SHIP`(전체 1회 복제). 앞의 둘은 일회성 비용 + k×질의당비용 구조
- **핵심 결과(모델 예측)**: k\*가 σ에 따라 1~4000회로 3자릿수 변동하고,
  **지연 최소화 기준과 egress 요금 최소화 기준의 답이 최대 ~35배 차이**.
  링크가 빠를수록 격차가 커지고(1Gbit σ=0.001에서 27 vs 1000),
  느린 링크에서는 두 기준이 거의 일치(100Mbit에서 206 vs 1000)
- **실험 설계 요령**: k를 스윕하지 않는다. 한 trial에서 K회 반복해 "일회성 비용"과
  "질의당 비용"을 분리 측정하면 모든 k의 곡선이 해석적으로 나온다.
  덕분에 네트워크 조건당 43 trial, 20~45분이면 끝남
- **핵심 기법**: Parquet footer만 읽어서(스캔 없이, ranged GET 몇 번)
  σ̂는 row group zone map(min/max)으로, π̂는 컬럼 청크 압축 크기로 추정
- 실제 코드는 이 저널에 커밋하지 않음. `바탕 화면/icngc_pushdown/README.md`에
  일자별 체크리스트와 "교차점이 안 생길 때의 대응"이 정리돼 있음

## 2026-09-05 — 논문주제찾기 (v0.0.0)

### 한 일

-

### 다음 할 일

-

### 참고할 맥락 / 결정 사항

-
