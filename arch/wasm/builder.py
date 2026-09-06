# xphi.arch.wasm.builder
import os
import shutil
import json
import time
import asyncio
from pathlib import Path
from typing import List, Dict, Optional

from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.tracer.bound import BaseTracer
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("wasm.builder")

THEORIA_ROOT = resolve_path("theoria")
TIME_ROOT = resolve_path("time")
REGISTRY_FILE = TIME_ROOT / "registry.json"

WASM_PROJECTS = [
    {
        "name": "dphi",
        "env": {"RUSTFLAGS": "-C target-feature=+simd128 -C opt-level=3"}
    },
    {
        "name": "dvm",
        "env": {"RUSTFLAGS": "-C opt-level=3"}
    },
    {
        "name": "gateway", 
        # [FIX] 컴파일 랙(Stuttering) 제거: fat -> thin 으로 변경하고 codegen-units 제한을 해제하여 멀티코어 빌드 허용
        "env": {"RUSTFLAGS": "-C opt-level=3 -C lto=thin"}
    },
]


class WasmBuilder(BaseTracer):
    """WASM 컴파일 (Dphi, DVM, Gateway) 및 Rust-Driven JSON 스키마 자동 추출 페이즈"""
    
    def __init__(self, timeout: int = 120, target_projects: Optional[List[str]] = None):
        super().__init__(tracer_name="wasm.builder", timeout=timeout)
        self.build_error = ""
        self.target_projects = target_projects
        
        cargo_path = str(Path.home() / ".cargo" / "bin")
        if cargo_path not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{cargo_path}:{os.environ.get('PATH', '')}"

    def _write_schemas_sync(self, reg_data: dict, schemas: dict) -> None:
        with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
            json.dump(reg_data, f, separators=(',', ':'))
            
        schema_file = TIME_ROOT / "schema.json"
        with open(schema_file, "w", encoding="utf-8") as f:
            json.dump(schemas, f, separators=(',', ':'))

    async def generate_schema_from_rust(self) -> bool:
        self.log.info("[Builder] Extracting JSON Schema using Standard Binary...")
        await asyncio.to_thread(os.makedirs, TIME_ROOT, exist_ok=True)
        
        dphi_dir = THEORIA_ROOT / "dphi"
        
        code, out, err = await self.boundary.run_command(
            ["cargo", "run", "--bin", "schema", "--quiet"], 
            cwd=str(dphi_dir), capture=True
        )
        
        if code != 0:
            self.build_error = f"Schema Binary Failed (Exit code: {code})"
            self.log.error(f"[ERROR] {self.build_error}")
            return False

        try:
            schemas = await asyncio.to_thread(json.loads, out.strip())
            methods_list = schemas.get("Method", {}).get("enum", [])
            
            reg_data = {
                "generated_at": time.time(),
                "methods": methods_list,
                "schema_version": "Draft-07"
            }
            
            await asyncio.to_thread(self._write_schemas_sync, reg_data, schemas)
            self.log.info(f"[Builder] Standard Schema Extraction Complete ({len(methods_list)} methods).")
            return True
            
        except Exception as e:
            self.log.error(f"[ERROR] Unexpected error during schema processing: {e}")
            return False

    async def _compile_wasm_project(self, project_dir: Path, name: str, custom_env: dict = None) -> bool:
        """개별 Rust 프로젝트를 WASM으로 컴파일하는 헬퍼 메서드"""
        if not project_dir.exists():
            self.log.error(f"[Builder] Project directory not found: {project_dir}")
            return False

        # [CRITICAL FIX] 1. 전역 환경변수 완벽 백업
        original_env = os.environ.copy()
        
        try:
            # 2. 파이썬 프레임워크 제약(env 파라미터 미지원) 돌파를 위해 os.environ 직접 주입
            if custom_env:
                os.environ.update(custom_env)
                self.log.info(f"[Builder] Applying env for {name}: {custom_env}")

            # 3. Pest Macro 캐시 무효화를 위한 선제적 Clean (1:1 Syntax Error 재발 방지)
            if name == "gateway":
                self.log.info(f"[Builder] Wiping Cargo cache for '{name}' to force Pest Macro re-expansion...")
                # env 파라미터 없이 실행
                await self.boundary.run_command(
                    ["cargo", "clean", "-p", name], 
                    cwd=str(project_dir), capture=True
                )

            self.log.info(f"[Builder] Compiling {name} to wasm32-unknown-unknown...")
            
            # env 파라미터 없이 실행 (os.environ에 이미 적용됨)
            code, out, err = await self.boundary.run_command(
                ["cargo", "build", "--target", "wasm32-unknown-unknown", "--release"], 
                cwd=str(project_dir), 
                capture=True
            )
            
            if code != 0:
                self.build_error = err
                self.log.error(f"[Builder] {name} Compilation Failed:\n{err.strip() if err else 'No error message'}")
                
                # SIMD Fallback 처리
                if custom_env and "simd128" in custom_env.get("RUSTFLAGS", ""):
                    self.log.warning(f"[Builder] SIMD Compilation failed. Retrying without SIMD optimization...")
                    
                    os.environ.clear()
                    fallback_env = original_env.copy()
                    fallback_env["RUSTFLAGS"] = "-C opt-level=3"
                    os.environ.update(fallback_env)
                    
                    # env 파라미터 없이 실행
                    code, out, err = await self.boundary.run_command(
                        ["cargo", "build", "--target", "wasm32-unknown-unknown", "--release"], 
                        cwd=str(project_dir), 
                        capture=True
                    )
                    
                    if code == 0:
                        self.log.info(f"[Builder] Fallback compilation successful (without SIMD).")
                        return True
                    else:
                        self.log.error(f"[Builder] Fallback Compilation Failed too:\n{err.strip()}")

                return False
                
            return True
            
        finally:
            # 4. [매우 중요] 컴파일 성공/실패 여부와 관계없이 원래 환경변수로 완벽히 복구하여 비동기 오염 방지
            os.environ.clear()
            os.environ.update(original_env)

    async def build_and_deploy(self, projects_to_build: List[Dict]) -> bool:
        await self.boundary.run_command(
            ["rustup", "target", "add", "wasm32-unknown-unknown"], 
            cwd=str(THEORIA_ROOT) 
        )
        
        await asyncio.to_thread(os.makedirs, TIME_ROOT, exist_ok=True)

        for proj in projects_to_build:
            proj_name = proj["name"]
            proj_env = proj.get("env", None)
            proj_dir = THEORIA_ROOT / proj_name
            
            if not await self._compile_wasm_project(proj_dir, proj_name, proj_env):
                return False

            build_file = proj_dir / "target" / "wasm32-unknown-unknown" / "release" / f"{proj_name}.wasm"
            dest_file = TIME_ROOT / f"{proj_name}.wasm"

            try:
                await asyncio.to_thread(shutil.copy2, build_file, dest_file)
                self.log.info(f"[Builder] Copied artifact -> {dest_file.name}")
            except FileNotFoundError as e:
                self.log.error(f"[Builder] Deployment Failed for {proj_name}. Artifact not found: {e}")
                return False

        return True

    async def execute(self) -> None:
        projects_to_build = [
            p for p in WASM_PROJECTS 
            if not self.target_projects or p["name"] in self.target_projects
        ]
        project_names = [p["name"] for p in projects_to_build]
        
        self.log.info(f"\n--- [START] Compiling WASM Artifacts ({', '.join(project_names).upper()}) ---")
        
        if not self.target_projects or "dphi" in self.target_projects:
            if not await self.generate_schema_from_rust():
                self.rupture_confirmed = True
                return
        else:
            self.log.info(f"[Builder] Skipping Schema Extraction (Not required for {project_names}).")
            
        if not await self.build_and_deploy(projects_to_build):
            self.rupture_confirmed = True


if __name__ == "__main__":
    async def main():
        builder = WasmBuilder()
        log.info("Starting WasmBuilder (Full Mode)...")
        await builder.execute()
        
        if getattr(builder, 'rupture_confirmed', False):
            log.info("[Main] Build failed. Check the logs for details.")
        else:
            log.info("[Main] Build and deployment completed successfully!")

    asyncio.run(main())