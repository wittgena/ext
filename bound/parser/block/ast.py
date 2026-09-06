# xphi.bound.parser.block.ast
## @lineage: xphi.xor.parser.block.ast
## @lineage: xphi.xor.parser.ast
## @lineage: xphi.arch.xor.parser.ast
import re
import ast
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional, Union, Dict, Tuple

from xphi.bound.parser.block.schema import (
    MdDocument, 
    MdSection, 
    MdNode, 
    Heading, 
    CodeBlock, 
    Paragraph, 
    Contract
)

class KtAstParser:
    def __init__(self, path: Path, contracts: List[Contract] = None):
        self.path = path
        self.contracts = contracts or []

    def parse(self) -> MdDocument:
        doc = MdDocument(path=self.path, sections=[])

        try:
            lines = self.path.read_text(encoding="utf-8").splitlines()
        except Exception as e:
            section = MdSection(title="error", level=1, meta_tag="")
            section.children.append(Paragraph(text=f"Failed to read file: {e}"))
            doc.sections.append(section)
            return doc

        if not self.contracts:
            section = MdSection(title="file_root", level=1, meta_tag="")
            section.children.append(CodeBlock(lang="kotlin", content="\n".join(lines)))
            doc.sections.append(section)
            return doc

        # 1. location에서 (시작줄, 끝줄) 파싱
        parsed_contracts: List[Tuple[int, int, Contract]] = []
        for f in self.contracts:
            start_line, end_line = self._parse_location(f.location, len(lines))
            parsed_contracts.append((start_line, end_line, f))

        # 2. 정렬: 시작줄은 오름차순, 끝줄은 내림차순 
        # (이렇게 해야 큰 범위인 Class가 먼저 오고, 내부의 Function이 뒤에 옴)
        parsed_contracts.sort(key=lambda x: (x[0], -x[1]))

        root_sections = []
        stack = []  # 계층 구조(Nesting) 추적을 위한 스택

        for start_line, end_line, fact in parsed_contracts:
            kind = fact.kind or "block"
            name = fact.name or "unnamed"
            title = f"{kind}::{name}"
            meta_tag = self._build_meta_tag(fact)

            start_idx = max(0, start_line - 1)
            end_idx = min(len(lines), end_line)
            block_content = "\n".join(lines[start_idx:end_idx]).strip()

            if not block_content:
                continue

            # 현재 블록의 시작줄이 스택 최상단 블록의 끝줄을 벗어났다면 (형제 노드 혹은 상위 노드로 이동)
            while stack and stack[-1][0] < start_line:
                stack.pop()

            level = len(stack) + 1
            section = MdSection(title=title, level=level, meta_tag=meta_tag)
            section.children.append(CodeBlock(lang="kotlin", content=block_content))

            if stack:
                # 스택에 부모가 있다면 부모의 subsection으로 편입 (예: 클래스 하위의 함수)
                stack[-1][1].subsections.append(section)
            else:
                # 부모가 없다면 최상위 노드
                root_sections.append(section)

            # 현재 섹션을 스택에 푸시 (종료줄, 섹션 객체)
            stack.append((end_line, section))

        doc.sections.extend(root_sections)
        return doc

    def _parse_location(self, location: str, max_lines: int) -> Tuple[int, int]:
        """
        포맷 "start:end" 를 파싱. 매칭 실패시 기본값 반환.
        """
        if not location:
            return 1, max_lines
        
        m = re.match(r'^(\d+):(\d+)$', location)
        if m:
            return int(m.group(1)), int(m.group(2))
            
        m_single = re.match(r'^(\d+)$', location)
        if m_single:
            return int(m_single.group(1)), int(m_single.group(1))
            
        return 1, max_lines

    def _build_meta_tag(self, fact: Contract) -> str:
        meta = []
        if fact.features:
            meta.append(f"features=[{','.join(fact.features)}]")
        if fact.refs:
            meta.append(f"refs=[{','.join(fact.refs)}]")
            
        return " | ".join(meta) if meta else ""


## Markdown → AST
class MdAstParser:
    def __init__(self, source: str, is_file: bool = True):
        """
        :param source: 파일 경로(str) 또는 마크다운 텍스트 원문(str)
        :param is_file: True면 source를 파일 경로로 인식, False면 메모리 상의 텍스트로 인식
        """
        self.is_file = is_file
        if self.is_file:
            self.path = Path(source)
            self.raw_text = None
        else:
            self.path = Path("<memory_topology>")  # MdDocument 호환성을 위한 가상 Path
            self.raw_text = source

    def parse(self) -> MdDocument:
        # 파일/메모리 분기 처리
        if self.is_file:
            if not self.path.exists():
                raise FileNotFoundError(f"File not found: {self.path}")
            text = self.path.read_text(encoding="utf-8")
        else:
            text = self.raw_text

        lines = text.splitlines()

        root_sections: List[MdSection] = []
        section_stack: List[MdSection] = []
        current_section: Optional[MdSection] = None

        inside_code = False
        code_lang = ""
        code_buffer: List[str] = []
        paragraph_buffer: List[str] = []

        def flush_paragraph():
            nonlocal paragraph_buffer
            if paragraph_buffer and current_section:
                content = "\n".join(paragraph_buffer).strip()
                if content and content != "---":
                    current_section.children.append(Paragraph(content))
            paragraph_buffer = []

        for line in lines:
            ## Heading
            heading_match = re.match(r"(#{1,6})\s+(.*)", line)
            if heading_match and not inside_code:
                flush_paragraph()

                level = len(heading_match.group(1))
                title = heading_match.group(2).strip()
                meta_tag = title.startswith("@")
                new_section = MdSection(level, title, meta_tag)
                new_section.children.append(Heading(level=level, content=title))

                while section_stack and section_stack[-1].level >= level:
                    section_stack.pop()

                if section_stack:
                    section_stack[-1].subsections.append(new_section)
                else:
                    root_sections.append(new_section)

                section_stack.append(new_section)
                current_section = new_section
                continue

            ## Code block start / end
            code_match = re.match(r"```([\w\.\-]+)?", line.strip())
            if code_match:
                if not inside_code:
                    flush_paragraph()
                    inside_code = True
                    code_lang = code_match.group(1) or "plain"
                    code_buffer = []
                else:
                    inside_code = False
                    if current_section:
                        current_section.children.append(
                            CodeBlock(code_lang, "\n".join(code_buffer).strip())
                        )
                    code_buffer = []
                continue

            if inside_code:
                code_buffer.append(line)
                continue

            ## Paragraph
            if current_section:
                if line.strip() == "":
                    flush_paragraph()
                else:
                    paragraph_buffer.append(line)

        flush_paragraph()
        return MdDocument(self.path, root_sections)


## Python → AST (using Python builtin ast module)
class PyAstParser:
    def __init__(self, path: Union[str, Path]):
        self.path = Path(path)

    def parse(self) -> MdDocument:
        if not self.path.exists():
            raise FileNotFoundError(f"File not found: {self.path}")

        source_code = self.path.read_text(encoding="utf-8")
        lines = source_code.splitlines(keepends=True)
        
        # 파일 전체를 감싸는 Root Section
        root_section = MdSection(level=1, title=self.path.name, meta_tag=False)
        
        # 1. 메타데이터 추출 (기존 로직 유지 - main 함수의 docstring)
        docstring = self._extract_main_docstring(lines)
        if docstring:
            metadata = self._parse_docstring(docstring)
            for key, value in metadata.items():
                meta_section = MdSection(level=2, title=f"@{key}", meta_tag=True)
                content = value.strip()
                if content:
                    meta_section.children.append(Paragraph(text=content))
                root_section.subsections.append(meta_section)

        # 2. Python AST 파싱을 통한 자동 청킹 (Chunking)
        try:
            tree = ast.parse(source_code)
            self._split_into_blocks(tree, source_code, root_section)
        except SyntaxError as e:
            # 문법 오류가 있는 파일은 Fallback으로 전체를 하나의 블록으로 처리
            err_section = MdSection(level=2, title="py.script (syntax_error)", meta_tag=False)
            err_section.children.append(CodeBlock(lang="python", content=source_code.strip()))
            root_section.subsections.append(err_section)

        return MdDocument(self.path, [root_section])

    def _split_into_blocks(self, tree: ast.Module, source_code: str, root_section: MdSection):
        """AST 노드를 순회하며 클래스, 함수, 전역 코드를 별도의 Section으로 분리합니다."""
        global_lines = []

        for node in tree.body:
            # Python 3.8+ 이상에서 지원하는 get_source_segment 활용
            node_code = ast.get_source_segment(source_code, node)
            if not node_code:
                continue

            if isinstance(node, ast.ClassDef):
                # 클래스 블록
                sec = MdSection(level=2, title=f"class:{node.name}", meta_tag=False)
                sec.children.append(CodeBlock(lang="python", content=node_code))
                root_section.subsections.append(sec)

            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                # 함수 블록 (main 함수도 여기 포함됨)
                sec = MdSection(level=2, title=f"func:{node.name}", meta_tag=False)
                sec.children.append(CodeBlock(lang="python", content=node_code))
                root_section.subsections.append(sec)

            else:
                # Import, 전역 변수 등 모듈 레벨 코드 수집
                global_lines.append(node_code)

        # 수집된 모듈 레벨 코드가 있다면 하나의 Section으로 병합
        if global_lines:
            sec = MdSection(level=2, title="module_globals", meta_tag=False)
            sec.children.append(CodeBlock(lang="python", content="\n".join(global_lines)))
            # 전역 코드는 가독성을 위해 맨 앞으로(메타데이터 바로 뒤) 삽입하는 것도 좋은 방법입니다.
            root_section.subsections.insert(len(root_section.subsections), sec)

    def _extract_main_docstring(self, lines: List[str]) -> str:
        doc_lines = []
        in_doc = False
        for i, line in enumerate(lines):
            if line.strip().startswith("def main("):
                for j in range(i + 1, len(lines)):
                    l = lines[j].strip()
                    if l.startswith('"""') or l.startswith("'''"):
                        if not in_doc:
                            in_doc = True
                            doc_lines.append(l.lstrip("\"'"))
                            # 한 줄짜리 docstring 처리
                            if (l.endswith('"""') and len(l) > 3) or (l.endswith("'''") and len(l) > 3):
                                doc_lines[-1] = doc_lines[-1].rstrip("\"'")
                                break
                        else:
                            doc_lines.append(l.rstrip("\"'"))
                            break
                    elif in_doc:
                        doc_lines.append(l)
                break
        return "\n".join(doc_lines)

    def _parse_docstring(self, doc: str) -> Dict[str, str]:
        sections = {}
        current = None
        for line in doc.splitlines():
            line = line.strip()
            if line.startswith("@"):
                tag, _, value = line.partition(":")
                current = tag.strip("@")
                sections[current] = value.strip()
            elif current and line:
                sections[current] += "\n" + line
        return sections


class PyDotMdParser:
    """
    @py.start ~ @py.end 마커 기반의 정적 스크립트 추출 및 
    main() 함수의 docstring 메타데이터를 파싱하여 MdDocument 모델로 변환하는 클래스
    """
    def __init__(self, path: Union[str, Path]):
        self.path = Path(path)

    def parse(self) -> MdDocument:
        if not self.path.exists():
            raise FileNotFoundError(f"File not found: {self.path}")

        lines = self.path.read_text(encoding="utf-8").splitlines(keepends=True)
        
        # 1. @py.start ~ @py.end 블록 추출
        code_lines = self._extract_py_block(lines)
        code_content = "".join(code_lines).strip()
        
        # 2. main 함수 docstring 기반 메타데이터 추출
        docstring = self._extract_main_docstring(lines)
        metadata = self._parse_docstring(docstring)
        
        # 3. MdDocument 객체 모델 구축
        root_section = MdSection(level=1, title=self.path.name, meta_tag=False)
        
        # 지정된 순서대로 메타데이터 Section 추가 (desc, input, output, example 등)
        target_keys = ["desc", "input", "output", "example"]
        for key in target_keys:
            if key in metadata:
                meta_section = MdSection(level=2, title=f"@{key}", meta_tag=True)
                meta_section.children.append(Paragraph(text=metadata[key].strip()))
                root_section.subsections.append(meta_section)
                
        # 기본 키 외에 추가로 파싱된 메타데이터가 있다면 덧붙임
        for key, value in metadata.items():
            if key not in target_keys:
                meta_section = MdSection(level=2, title=f"@{key}", meta_tag=True)
                meta_section.children.append(Paragraph(text=value.strip()))
                root_section.subsections.append(meta_section)

        # 4. py.script Section에 추출된 코드 블록 추가
        script_section = MdSection(level=2, title="py.script", meta_tag=False)
        if code_content:
            script_section.children.append(CodeBlock(lang="python", content=code_content))
        root_section.subsections.append(script_section)

        return MdDocument(self.path, [root_section])

    def _extract_py_block(self, lines: List[str]) -> List[str]:
        in_block = False
        collected = []

        for line in lines:
            if "# @py.start" in line:
                in_block = True

            if in_block:
                collected.append(line)

            if in_block and "# @py.end" in line:
                break

        return collected

    def _extract_main_docstring(self, lines: List[str]) -> str:
        doc_lines = []
        in_doc = False
        for i, line in enumerate(lines):
            if line.strip().startswith("def main("):
                for j in range(i + 1, len(lines)):
                    l = lines[j].strip()
                    if l.startswith('"""') or l.startswith("'''"):
                        if not in_doc:
                            in_doc = True
                            doc_lines.append(l.lstrip("\"'"))
                            # 한 줄짜리 docstring 처리
                            if (l.endswith('"""') and len(l) > 3) or (l.endswith("'''") and len(l) > 3):
                                doc_lines[-1] = doc_lines[-1].rstrip("\"'")
                                break
                        else:
                            doc_lines.append(l.rstrip("\"'"))
                            break
                    elif in_doc:
                        doc_lines.append(l)
                break
        return "\n".join(doc_lines)

    def _parse_docstring(self, doc: str) -> Dict[str, str]:
        sections = {}
        current = None
        for line in doc.splitlines():
            line = line.strip()
            if line.startswith("@"):
                tag, _, value = line.partition(":")
                current = tag.strip("@")
                sections[current] = value.strip()
            elif current and line:
                sections[current] += "\n" + line
        return sections