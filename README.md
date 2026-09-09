# xphi.README
@desc: Core Infrastructure Kernel

**XPHI**는 결정론적(Deterministic) 코어 커널 및 분산 상태 관리 엔진입니다.
Fiber가 외부 LLM 연동 및 게이트웨이 프록시를 담당한다면, XPHI는 하드웨어 수준의 WASM 격리, FSM(유한 상태 기계) 기반의 상태 제어, 그리고 관측망(Observability)을 통해 인프라의 안정성을 물리적으로 보장합니다.

## 🏗 Directory Structure

XPHI는 기능적 격리를 위해 5개의 핵심 네임스페이스로 구성됩니다.

* **`kernel/` (Core Execution):** 하이퍼바이저 수준의 리소스 제어(cgroup), WASM 네이티브 샌드박싱 및 데몬/태스크 생명주기 관리.
* **`state/` (Determinism & Runtime):** 레이스 컨디션을 방지하는 엄격한 FSM(유한 상태 기계), 상태 합의(Consensus) 원장 및 다중 런타임 제어.
* **`bound/` (Boundary & Oracles):** 외부 오라클(Binance, Coinbase) 연동, 암호화된 시크릿 관리(Vault) 및 외부 코드 격리(Sandbox) 프로토콜.
* **`watcher/` (Control Plane):** Secure MCP 통신 서버, 분산 트레이싱, 카오스 엔지니어링(결함 주입 테스트) 및 메트릭 관측망.
* **`arch/` (Contracts & Models):** 분산 노드 간의 비동기 이벤트 메쉬(Mesh), 스마트 컨트랙트 레지스트리 및 공통 데이터 모델 정의.

## 🚀 Key Features

* **Zero-Trust Sandboxing:** 메모리 누수와 탈취를 완벽히 차단하는 일회성 격리(Ephemeral) 런타임 환경 제공.
* **Deterministic FSM Engine:** 모든 에이전트 인텐트와 트랜잭션을 예측 가능한 순차적 상태 기계로 제어.
* **Secure MCP Backend:** 외부로부터의 취약한 페이로드를 결정론적 시스템 이벤트(LogicStream)로 안전하게 변환.
* **Built-in Chaos Resilience:** 트레이서를 통한 OOM(Out of Memory) 트랩 등 결함 주입 테스트로 인프라 복원력 상시 검증.