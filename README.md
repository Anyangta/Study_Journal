# Study Journal

여러 로컬 환경(PC A, PC B, Claude Code on the web 등)을 오가며 Claude Code로 작업할 때,
세션의 맥락과 진행 상황을 이어가기 위한 **작업 기억 저장소**입니다.

## 왜 필요한가

Claude Code 세션은 로컬 환경이 바뀌면 이전 대화나 작업 맥락을 알 수 없습니다.
이 저장소에 세션별 작업 기록(저널)을 남겨두면, 다른 로컬에서 이 저장소를 pull 받아
Claude Code에게 최근 기록을 읽게 함으로써 마치 이어서 작업하는 것처럼 재개할 수 있습니다.

## 사용 방법

### 새 로컬에서 작업을 이어갈 때

1. 이 저장소를 clone/pull 합니다.
   ```bash
   git clone https://github.com/Anyangta/Study_Journal.git
   # 이미 있다면
   git pull
   ```
2. 이 저장소 안(또는 이 저장소를 참조하며)에서 Claude Code를 실행하면 `CLAUDE.md`를
   자동으로 읽고, `journal/INDEX.md`와 최근 기록을 참고해 맥락을 파악합니다.
3. 필요하면 Claude에게 "저널 읽고 이어서 진행해줘"라고 요청하세요.

### 작업이 끝났을 때 (또는 중요한 진행이 있을 때)

Claude에게 "오늘 작업 저널에 기록하고 push해줘"라고 요청하면:
1. `journal/YYYY-MM-DD-주제.md` 형식으로 새 기록을 작성
2. `journal/INDEX.md`에 요약 한 줄 추가
3. commit 후 push

## 폴더 구조

```
journal/
  INDEX.md              # 전체 기록 목차 (최신순)
  YYYY-MM-DD-주제.md     # 개별 세션 기록
templates/
  entry-template.md     # 새 기록 작성용 템플릿
CLAUDE.md                # Claude Code용 자동 동작 지침
```

## 기록 작성 규칙

- 각 기록에는 관련 프로젝트/저장소, 브랜치, 한 작업 내용, 다음 할 일, 중요한 결정/맥락을 남깁니다.
- **실제 코드는 이 저장소에 넣지 않습니다.** 이 저장소는 "기록"만 보관하고, 실제 프로젝트는 각자의 저장소에 있습니다.
